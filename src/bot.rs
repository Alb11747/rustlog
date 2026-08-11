use crate::{
    app::App,
    db::schema::{StructuredMessage, UnstructuredMessage},
    logs::extract::{extract_channel_and_user_from_raw, extract_raw_timestamp},
    recent_messages::RecentMessagesClient,
    ShutdownRx,
};
use anyhow::{anyhow, Context};
use chrono::Utc;
use lazy_static::lazy_static;
use moka::sync::Cache;
use prometheus::{register_int_counter_vec, IntCounterVec};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
    time::sleep,
};
use tracing::{debug, error, info, log::warn, trace};
use twitch_irc::{
    login::LoginCredentials,
    message::{AsRawIRC, IRCMessage, ServerMessage},
    ClientConfig, SecureTCPTransport, TwitchIRCClient,
};

const CHANNEL_REJOIN_INTERVAL_SECONDS: u64 = 3600;
const CHANNELS_REFETCH_RETRY_INTERVAL_SECONDS: u64 = 5;
const RECENT_MESSAGE_DEDUPE_TTL_SECONDS: u64 = 30;
const RECENT_MESSAGE_DEDUPE_CAPACITY: u64 = 50_000;
const RECENT_MESSAGE_INFLIGHT_CAPACITY: u64 = 10_000;
const RECENT_MESSAGE_INFLIGHT_TTL_SECONDS: u64 = 30;

type TwitchClient<C> = TwitchIRCClient<SecureTCPTransport, C>;

#[derive(Debug)]
pub enum BotMessage {
    JoinChannels(Vec<String>),
    PartChannels(Vec<String>),
}

lazy_static! {
    static ref MESSAGES_RECEIVED_COUNTERS: IntCounterVec = register_int_counter_vec!(
        "rustlog_messages_received",
        "How many messages were written",
        &["channel_id"]
    )
    .unwrap();
}

const COMMAND_PREFIX: &str = "!rustlog ";

pub async fn run<C: LoginCredentials>(
    login_credentials: C,
    app: App,
    writer_tx: Sender<StructuredMessage<'static>>,
    shutdown_rx: ShutdownRx,
    command_rx: Receiver<BotMessage>,
) {
    let bot = Bot::new(app, writer_tx);
    bot.run(login_credentials, shutdown_rx, command_rx).await;
}

#[derive(Clone)]
struct Bot {
    app: App,
    writer_tx: Sender<StructuredMessage<'static>>,
    recent_messages: RecentMessagesClient,
    recent_message_dedupe: Cache<String, ()>,
    backfill_inflight: Cache<String, ()>,
    backfill_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    #[cfg(test)]
    skip_existing_message_lookup: bool,
}

struct PreparedMessage {
    message: StructuredMessage<'static>,
    dedupe_key: String,
}

impl Bot {
    pub fn new(app: App, writer_tx: Sender<StructuredMessage<'static>>) -> Bot {
        Self::new_with_recent_messages(app, writer_tx, RecentMessagesClient::from_env())
    }

    fn new_with_recent_messages(
        app: App,
        writer_tx: Sender<StructuredMessage<'static>>,
        recent_messages: RecentMessagesClient,
    ) -> Bot {
        Self {
            app,
            writer_tx,
            recent_messages,
            recent_message_dedupe: Cache::builder()
                .time_to_live(Duration::from_secs(RECENT_MESSAGE_DEDUPE_TTL_SECONDS))
                .max_capacity(RECENT_MESSAGE_DEDUPE_CAPACITY)
                .build(),
            backfill_inflight: Cache::builder()
                .time_to_live(Duration::from_secs(RECENT_MESSAGE_INFLIGHT_TTL_SECONDS))
                .max_capacity(RECENT_MESSAGE_INFLIGHT_CAPACITY)
                .build(),
            backfill_tasks: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            skip_existing_message_lookup: false,
        }
    }

    pub async fn run<C: LoginCredentials>(
        self,
        login_credentials: C,
        mut shutdown_rx: ShutdownRx,
        mut command_rx: Receiver<BotMessage>,
    ) {
        let own_login = match login_credentials.get_credentials().await {
            Ok(credentials) => Some(credentials.login),
            Err(err) => {
                warn!("Could not determine IRC login for recent-message backfill: {err}");
                None
            }
        };
        let client_config = ClientConfig::new_simple(login_credentials);
        let (mut receiver, client) = TwitchIRCClient::<SecureTCPTransport, C>::new(client_config);

        let app = self.app.clone();
        let join_client = client.clone();
        tokio::spawn(async move {
            loop {
                let channel_ids = app.config.channels.read().unwrap().clone();

                let interval = match app
                    .get_users(Vec::from_iter(channel_ids), vec![], true)
                    .await
                {
                    Ok(users) => {
                        info!("Joining {} channels", users.len());
                        for channel_login in users.into_values() {
                            debug!("Logging channel {channel_login}");
                            join_client
                                .join(channel_login)
                                .expect("Failed to join channel");
                        }
                        CHANNEL_REJOIN_INTERVAL_SECONDS
                    }
                    Err(err) => {
                        error!("Could not fetch users list: {err}");
                        CHANNELS_REFETCH_RETRY_INTERVAL_SECONDS
                    }
                };
                sleep(Duration::from_secs(interval)).await;
            }
        });

        let bot = self.clone();
        let msg_client = client.clone();
        tokio::spawn(async move {
            while let Some(msg) = command_rx.recv().await {
                match msg {
                    BotMessage::JoinChannels(channels) => {
                        if let Err(err) = bot
                            .update_channels(
                                &msg_client,
                                &channels.iter().map(String::as_str).collect::<Vec<_>>(),
                                ChannelAction::Join,
                            )
                            .await
                        {
                            error!("Could not join channels: {err}");
                        }
                    }
                    BotMessage::PartChannels(channels) => {
                        if let Err(err) = bot
                            .update_channels(
                                &msg_client,
                                &channels.iter().map(String::as_str).collect::<Vec<_>>(),
                                ChannelAction::Part,
                            )
                            .await
                        {
                            error!("Could not join channels: {err}");
                        }
                    }
                }
            }
        });

        loop {
            tokio::select! {
                Some(msg) = receiver.recv() => {
                    if let Err(e) = self.handle_message(msg, &client, own_login.as_deref()).await {
                        error!("Could not handle message: {e}");
                    }
                }
                _ = shutdown_rx.changed() => {
                    debug!("Shutting down bot task");
                    self.stop_backfill_tasks().await;
                    break;
                }
            }
        }
    }

    async fn handle_message<C: LoginCredentials>(
        &self,
        msg: ServerMessage,
        client: &TwitchClient<C>,
        own_login: Option<&str>,
    ) -> anyhow::Result<()> {
        if let Some(own_login) = own_login {
            self.trigger_backfill_for_own_join(&msg, own_login).await;
        }

        let raw_irc = msg.as_raw_irc();
        let prepared = self.prepare_message(IRCMessage::from(msg.clone()), &raw_irc)?;
        let Some(prepared) = prepared else {
            return Ok(());
        };
        if !self.claim_message(&prepared.dedupe_key) {
            return Ok(());
        }

        if let ServerMessage::Privmsg(privmsg) = &msg {
            trace!("Processing message {}", privmsg.message_text);
            if let Some(cmd) = privmsg.message_text.strip_prefix(COMMAND_PREFIX) {
                if let Err(err) = self
                    .handle_command(cmd, client, &privmsg.sender.id, &privmsg.sender.login)
                    .await
                {
                    warn!("Could not handle command {cmd}: {err:#}");
                }
            }
        }

        self.write_prepared(prepared).await?;

        Ok(())
    }

    async fn trigger_backfill_for_own_join(&self, msg: &ServerMessage, own_login: &str) {
        if let ServerMessage::Join(join) = msg {
            if join.user_login.eq_ignore_ascii_case(own_login) {
                self.trigger_recent_messages_fetch(join.channel_login.clone(), "successful join")
                    .await;
            }
        }
    }

    fn check_admin(&self, user_login: &str) -> anyhow::Result<()> {
        if self
            .app
            .config
            .admins
            .iter()
            .any(|login| login == user_login)
        {
            Ok(())
        } else {
            Err(anyhow!("User {user_login} is not an admin"))
        }
    }

    fn prepare_message(
        &self,
        irc_message: IRCMessage,
        raw_irc: &str,
    ) -> anyhow::Result<Option<PreparedMessage>> {
        if irc_message.command == "ROOMSTATE" {
            return Ok(None);
        }
        if let Some((channel_id, maybe_user_id)) = extract_channel_and_user_from_raw(&irc_message) {
            let timestamp = extract_raw_timestamp(&irc_message)
                .unwrap_or_else(|| Utc::now().timestamp_millis().try_into().unwrap());
            let user_id = maybe_user_id.unwrap_or_default().to_owned();
            let unstructured = UnstructuredMessage {
                channel_id,
                user_id: &user_id,
                timestamp,
                raw: raw_irc,
            };
            let message = StructuredMessage::from_unstructured(&unstructured)?.into_owned();
            let dedupe_key = structured_event_identity(&message);
            return Ok(Some(PreparedMessage {
                message,
                dedupe_key,
            }));
        }

        Ok(None)
    }

    fn claim_message(&self, dedupe_key: &str) -> bool {
        self.recent_message_dedupe
            .entry(dedupe_key.to_owned())
            .or_insert_with(|| ())
            .is_fresh()
    }

    async fn write_prepared(&self, prepared: PreparedMessage) -> anyhow::Result<()> {
        let message = prepared.message;
        if self
            .app
            .config
            .opt_out
            .contains_key(message.channel_id.as_ref())
            || self
                .app
                .config
                .opt_out
                .contains_key(message.user_id.as_ref())
        {
            return Ok(());
        }

        if !message.channel_id.is_empty() {
            MESSAGES_RECEIVED_COUNTERS
                .with_label_values(&[message.channel_id.as_ref()])
                .inc();
        }
        self.writer_tx.send(message).await?;
        Ok(())
    }

    async fn trigger_recent_messages_fetch(&self, channel_login: String, reason: &'static str) {
        if !self.recent_messages.enabled() {
            return;
        }

        let channel_login = channel_login.to_ascii_lowercase();
        if !self
            .backfill_inflight
            .entry(channel_login.clone())
            .or_insert_with(|| ())
            .is_fresh()
        {
            return;
        }

        let bot = self.clone();
        let handle = tokio::spawn(async move {
            if let Err(err) = bot.fetch_recent_messages_for_channel(&channel_login).await {
                warn!("Recent-message backfill failed for {channel_login} after {reason}: {err:#}");
            }
            bot.backfill_inflight.invalidate(&channel_login);
        });
        let mut tasks = self.backfill_tasks.lock().await;
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }

    async fn fetch_recent_messages_for_channel(&self, channel_login: &str) -> anyhow::Result<()> {
        let response = self.recent_messages.fetch(channel_login).await?;
        if response.error.is_some() || response.error_code.is_some() {
            warn!(
                "Recent-message backfill skipped for {channel_login}: error={:?} error_code={:?}",
                response.error, response.error_code
            );
            return Ok(());
        }

        let mut candidates = Vec::new();
        let mut response_keys = HashSet::new();
        let mut parse_failures = 0_usize;
        for raw in response.messages {
            let parsed = IRCMessage::parse(raw.trim().trim_matches('\0'))
                .map_err(anyhow::Error::from)
                .and_then(|message| self.prepare_message(message, &raw));
            match parsed {
                Ok(Some(prepared)) if response_keys.insert(prepared.dedupe_key.clone()) => {
                    candidates.push(prepared);
                }
                Ok(_) => {}
                Err(_) => parse_failures += 1,
            }
        }
        if parse_failures > 0 {
            warn!(
                "Recent-message backfill for {channel_login} skipped {parse_failures} malformed raw IRC line(s)"
            );
        }

        let existing_keys = self.existing_message_keys(&candidates).await?;
        for prepared in candidates {
            if existing_keys.contains(&prepared.dedupe_key)
                || !self.claim_message(&prepared.dedupe_key)
            {
                continue;
            }
            if let Err(err) = self.write_prepared(prepared).await {
                warn!("Could not store recent-message backfill event for {channel_login}: {err}");
            }
        }

        Ok(())
    }

    async fn existing_message_keys(
        &self,
        candidates: &[PreparedMessage],
    ) -> anyhow::Result<HashSet<String>> {
        #[cfg(test)]
        if self.skip_existing_message_lookup {
            return Ok(HashSet::new());
        }

        let candidate_keys = candidates
            .iter()
            .map(|prepared| prepared.dedupe_key.clone())
            .collect::<HashSet<_>>();
        if candidate_keys.is_empty() {
            return Ok(HashSet::new());
        }

        let mut existing_keys = HashSet::new();
        let mut groups = HashMap::<String, (u64, u64)>::new();
        for prepared in candidates {
            let entry = groups
                .entry(prepared.message.channel_id.to_string())
                .or_insert((prepared.message.timestamp, prepared.message.timestamp));
            entry.0 = entry.0.min(prepared.message.timestamp);
            entry.1 = entry.1.max(prepared.message.timestamp);
        }

        for (channel_id, (from_millis, to_millis)) in groups {
            let buffered = self
                .app
                .flush_buffer
                .messages_by_channel(
                    from_millis..to_millis.saturating_add(1),
                    &channel_id,
                )
                .await;
            existing_keys.extend(
                buffered
                    .iter()
                    .map(structured_event_identity)
                    .filter(|key| candidate_keys.contains(key)),
            );

            let stored_messages = self
                .app
                .db
                .query(
                    "SELECT ?fields FROM message_structured WHERE channel_id = ? AND timestamp >= ? AND timestamp <= ?",
                )
                .bind(channel_id)
                .bind(from_millis as f64 / 1000.0)
                .bind(to_millis as f64 / 1000.0)
                .fetch_all::<StructuredMessage<'static>>()
                .await
                .context("could not check stored recent-message identities")?;
            existing_keys.extend(
                stored_messages
                    .iter()
                    .map(structured_event_identity)
                    .filter(|key| candidate_keys.contains(key)),
            );
        }

        Ok(existing_keys)
    }

    async fn stop_backfill_tasks(&self) {
        let tasks = std::mem::take(&mut *self.backfill_tasks.lock().await);
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
    }

    async fn handle_command<C: LoginCredentials>(
        &self,
        cmd: &str,
        client: &TwitchClient<C>,
        sender_id: &str,
        sender_login: &str,
    ) -> anyhow::Result<()> {
        debug!("Processing command {cmd}");
        let mut split = cmd.split_whitespace();
        if let Some(action) = split.next() {
            let args: Vec<&str> = split.collect();

            match action {
                "join" => {
                    self.check_admin(sender_login)?;
                    self.update_channels(client, &args, ChannelAction::Join)
                        .await?
                }
                "leave" | "part" => {
                    self.check_admin(sender_login)?;
                    self.update_channels(client, &args, ChannelAction::Part)
                        .await?
                }
                "optout" => {
                    self.optout_user(&args, sender_login, sender_id).await?;
                }
                _ => (),
            }
        }

        Ok(())
    }

    async fn optout_user(
        &self,
        args: &[&str],
        sender_login: &str,
        sender_id: &str,
    ) -> anyhow::Result<()> {
        let arg = args.first().context("No optout code provided")?;
        if self.app.optout_codes.remove(*arg).is_some() {
            self.app.optout_user(sender_id).await?;

            Ok(())
        } else if self.check_admin(sender_login).is_ok() {
            let user_id = self.app.get_user_id_by_name(arg).await?;

            self.app.optout_user(&user_id).await?;

            Ok(())
        } else {
            Err(anyhow!("Invalid optout code"))
        }
    }

    async fn update_channels<C: LoginCredentials>(
        &self,
        client: &TwitchClient<C>,
        channels: &[&str],
        action: ChannelAction,
    ) -> anyhow::Result<()> {
        if channels.is_empty() {
            return Err(anyhow!("no channels specified"));
        }

        let channels = self
            .app
            .get_users(
                vec![],
                channels.iter().map(ToString::to_string).collect(),
                false,
            )
            .await?;

        {
            let mut config_channels = self.app.config.channels.write().unwrap();

            for (channel_id, channel_name) in channels {
                match action {
                    ChannelAction::Join => {
                        info!("Joining channel {channel_name}");
                        config_channels.insert(channel_id);
                        client.join(channel_name)?;
                    }
                    ChannelAction::Part => {
                        info!("Parting channel {channel_name}");
                        config_channels.remove(&channel_id);
                        client.part(channel_name);
                    }
                }
            }
        }

        self.app.config.save()?;

        Ok(())
    }
}

enum ChannelAction {
    Join,
    Part,
}

fn structured_event_identity(message: &StructuredMessage<'_>) -> String {
    if let Some(message_id) = message.uuid() {
        format!("uuid:{message_id}")
    } else {
        let canonical = serde_json::to_vec(message)
            .expect("serializing a structured message for deduplication cannot fail");
        format!("row:{}", blake3::hash(&canonical).to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        structured_event_identity, Bot, ServerMessage, RECENT_MESSAGE_INFLIGHT_TTL_SECONDS,
    };
    use crate::{
        app::{cache::UsersCache, App},
        config::Config,
        db::writer::FlushBuffer,
        recent_messages::{
            RecentMessagesClient, RecentMessagesRuntimeSummary, RECENT_MESSAGES_TIMEOUT,
        },
    };
    use axum::{
        extract::Path, http::StatusCode, response::IntoResponse, routing::get, Json, Router,
    };
    use dashmap::DashSet;
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };
    use tokio::{net::TcpListener, sync::mpsc, time::timeout};
    use twitch_api::{
        twitch_oauth2::{AccessToken, AppAccessToken, ClientId, ClientSecret},
        HelixClient,
    };
    use twitch_irc::message::IRCMessage;

    const REPLAYED_COMMAND: &str = "@room-id=1;user-id=200;tmi-sent-ts=1704067200000;id=robotty-command;display-name=admin;badges=;color=;user-type=;emotes=;flags= :admin!admin@admin.tmi.twitch.tv PRIVMSG #channelone :!rustlog optout secret-code";
    const UUID_MESSAGE: &str = "@room-id=1;user-id=200;tmi-sent-ts=1704067200000;id=272e342c-5864-4c59-b730-25908cdb7f57;display-name=user;badges=;color=;user-type=;emotes=;flags= :user!user@user.tmi.twitch.tv PRIVMSG #channelone :hello";

    fn test_app() -> App {
        let config: Config = serde_json::from_value(serde_json::json!({
            "clickhouseUrl": "http://127.0.0.1:9",
            "clickhouseDb": "rustlog",
            "channels": ["1"],
            "clientID": "client",
            "clientSecret": "secret",
            "admins": ["admin"],
            "optOut": {}
        }))
        .unwrap();
        let optout_codes = Arc::new(DashSet::new());
        optout_codes.insert("secret-code".to_string());
        App {
            helix_client: HelixClient::default(),
            token: Arc::new(AppAccessToken::from_existing_unchecked(
                AccessToken::new("token".to_string()),
                None,
                ClientId::new("client".to_string()),
                ClientSecret::new("secret".to_string()),
                None,
                None,
            )),
            users: UsersCache::default(),
            optout_codes,
            db: Arc::new(clickhouse::Client::default().with_url("http://127.0.0.1:9")),
            config: Arc::new(config),
            flush_buffer: FlushBuffer::default(),
        }
    }

    fn recent_messages_client(address: std::net::SocketAddr) -> RecentMessagesClient {
        RecentMessagesClient::from_summary(RecentMessagesRuntimeSummary {
            enabled: true,
            base_url: Some(format!("http://{address}/api/v2/recent-messages")),
            limit: 800,
            warnings: Vec::new(),
        })
    }

    async fn bot_with_server(
        router: Router,
    ) -> (
        Bot,
        mpsc::Receiver<crate::db::schema::StructuredMessage<'static>>,
        tokio::task::JoinHandle<Result<(), std::io::Error>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let (writer_tx, writer_rx) = mpsc::channel(10);
        let mut bot =
            Bot::new_with_recent_messages(test_app(), writer_tx, recent_messages_client(address));
        bot.skip_existing_message_lookup = true;
        (bot, writer_rx, server)
    }

    async fn wait_for_backfills(bot: &Bot) {
        let tasks = std::mem::take(&mut *bot.backfill_tasks.lock().await);
        for task in tasks {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn replay_stores_command_once_without_command_side_effects() {
        let router = Router::new().route(
            "/api/v2/recent-messages/{channel}",
            get(|| async {
                Json(serde_json::json!({
                    "messages": ["not raw irc", REPLAYED_COMMAND, REPLAYED_COMMAND],
                    "error": null,
                    "error_code": null
                }))
            }),
        );
        let (bot, mut writer_rx, server) = bot_with_server(router).await;

        bot.fetch_recent_messages_for_channel("channelone")
            .await
            .unwrap();

        let stored = timeout(Duration::from_secs(1), writer_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.user_friendly_text(), "!rustlog optout secret-code");
        assert!(bot.app.optout_codes.contains("secret-code"));
        assert!(!bot.app.config.opt_out.contains_key("200"));
        assert!(timeout(Duration::from_millis(50), writer_rx.recv())
            .await
            .is_err());
        server.abort();
    }

    #[tokio::test]
    async fn successful_own_joins_trigger_backfill_and_suppress_concurrent_fetches() {
        let requests = Arc::new(AtomicUsize::new(0));
        let handler_requests = requests.clone();
        let router = Router::new().route(
            "/api/v2/recent-messages/{channel}",
            get(move || {
                let requests = handler_requests.clone();
                async move {
                    requests.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Json(serde_json::json!({
                        "messages": [],
                        "error": null,
                        "error_code": null
                    }))
                }
            }),
        );
        let (bot, _writer_rx, server) = bot_with_server(router).await;
        let own_join = ServerMessage::try_from(
            IRCMessage::parse(
                ":justinfan12345!justinfan12345@justinfan12345.tmi.twitch.tv JOIN #channelone",
            )
            .unwrap(),
        )
        .unwrap();
        let other_join = ServerMessage::try_from(
            IRCMessage::parse(":viewer!viewer@viewer.tmi.twitch.tv JOIN #channelone").unwrap(),
        )
        .unwrap();

        bot.trigger_backfill_for_own_join(&own_join, "justinfan12345")
            .await;
        bot.trigger_backfill_for_own_join(&own_join, "justinfan12345")
            .await;
        bot.trigger_backfill_for_own_join(&other_join, "justinfan12345")
            .await;
        wait_for_backfills(&bot).await;
        assert_eq!(requests.load(Ordering::SeqCst), 1);

        bot.trigger_backfill_for_own_join(&own_join, "justinfan12345")
            .await;
        wait_for_backfills(&bot).await;
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        server.abort();
    }

    #[tokio::test]
    async fn triggering_a_backfill_prunes_finished_task_handles() {
        let router = Router::new().route(
            "/api/v2/recent-messages/{channel}",
            get(|| async {
                Json(serde_json::json!({
                    "messages": [],
                    "error": null,
                    "error_code": null
                }))
            }),
        );
        let (bot, _writer_rx, server) = bot_with_server(router).await;

        bot.trigger_recent_messages_fetch("first".to_string(), "test")
            .await;
        timeout(Duration::from_secs(1), async {
            loop {
                if bot
                    .backfill_tasks
                    .lock()
                    .await
                    .first()
                    .is_some_and(tokio::task::JoinHandle::is_finished)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        bot.trigger_recent_messages_fetch("second".to_string(), "test")
            .await;
        assert_eq!(bot.backfill_tasks.lock().await.len(), 1);

        wait_for_backfills(&bot).await;
        server.abort();
    }

    #[tokio::test]
    async fn stored_id_lookup_failure_prevents_backfill_insert() {
        let router = Router::new().route(
            "/api/v2/recent-messages/{channel}",
            get(|| async {
                Json(serde_json::json!({
                    "messages": [UUID_MESSAGE],
                    "error": null,
                    "error_code": null
                }))
            }),
        );
        let (mut bot, mut writer_rx, server) = bot_with_server(router).await;
        bot.skip_existing_message_lookup = false;

        assert!(bot
            .fetch_recent_messages_for_channel("channelone")
            .await
            .is_err());
        assert!(writer_rx.try_recv().is_err());

        server.abort();
    }

    #[tokio::test]
    async fn one_channel_failure_does_not_stop_another_backfill() {
        let router = Router::new().route(
            "/api/v2/recent-messages/{channel}",
            get(|Path(channel): Path<String>| async move {
                if channel == "broken" {
                    StatusCode::BAD_GATEWAY.into_response()
                } else {
                    Json(serde_json::json!({
                        "messages": [REPLAYED_COMMAND],
                        "error": null,
                        "error_code": null
                    }))
                    .into_response()
                }
            }),
        );
        let (bot, mut writer_rx, server) = bot_with_server(router).await;

        bot.trigger_recent_messages_fetch("broken".to_string(), "test")
            .await;
        bot.trigger_recent_messages_fetch("working".to_string(), "test")
            .await;
        wait_for_backfills(&bot).await;

        assert!(writer_rx.recv().await.is_some());
        server.abort();
    }

    #[tokio::test]
    async fn replay_dedupes_against_a_live_message_and_skips_remote_errors() {
        let router = Router::new().route(
            "/api/v2/recent-messages/{channel}",
            get(|Path(channel): Path<String>| async move {
                if channel == "remoteerror" {
                    Json(serde_json::json!({
                        "messages": [REPLAYED_COMMAND],
                        "error": "temporarily unavailable",
                        "error_code": "503"
                    }))
                } else {
                    Json(serde_json::json!({
                        "messages": [REPLAYED_COMMAND],
                        "error": null,
                        "error_code": null
                    }))
                }
            }),
        );
        let (bot, mut writer_rx, server) = bot_with_server(router).await;
        let message = IRCMessage::parse(REPLAYED_COMMAND).unwrap();
        let prepared = bot
            .prepare_message(message, REPLAYED_COMMAND)
            .unwrap()
            .unwrap();
        assert!(bot.claim_message(&prepared.dedupe_key));
        bot.write_prepared(prepared).await.unwrap();

        bot.fetch_recent_messages_for_channel("channelone")
            .await
            .unwrap();
        bot.fetch_recent_messages_for_channel("remoteerror")
            .await
            .unwrap();

        assert!(writer_rx.recv().await.is_some());
        assert!(timeout(Duration::from_millis(50), writer_rx.recv())
            .await
            .is_err());
        server.abort();
    }

    #[test]
    fn event_identity_uses_uuid_and_structured_hash_fallback() {
        let with_id = crate::db::schema::StructuredMessage::from_unstructured(
            &crate::db::schema::UnstructuredMessage {
                channel_id: "1",
                user_id: "200",
                timestamp: 1_704_067_200_000,
                raw: UUID_MESSAGE,
            },
        )
        .unwrap();
        assert_eq!(
            structured_event_identity(&with_id),
            "uuid:272e342c-5864-4c59-b730-25908cdb7f57"
        );

        let without_id = crate::db::schema::StructuredMessage::from_unstructured(
            &crate::db::schema::UnstructuredMessage {
                channel_id: "1",
                user_id: "200",
                timestamp: 1_704_067_200_000,
                raw: REPLAYED_COMMAND,
            },
        )
        .unwrap();
        let identity = structured_event_identity(&without_id);
        assert!(identity.starts_with("row:"));
        assert_eq!(identity, structured_event_identity(&without_id));
    }

    #[test]
    fn backfill_inflight_ttl_exceeds_request_timeout() {
        assert!(Duration::from_secs(RECENT_MESSAGE_INFLIGHT_TTL_SECONDS) > RECENT_MESSAGES_TIMEOUT);
    }
}

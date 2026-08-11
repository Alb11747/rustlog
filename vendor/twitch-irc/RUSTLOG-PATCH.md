# Rustlog patch provenance

This directory contains the published `twitch-irc` 5.0.1 crate from crates.io.
The upstream project is <https://github.com/robotty/twitch-irc-rs> and is licensed
under the MIT license included in this directory.

Rustlog moves the connection rate-limit permit cooldown ahead of transport
initialization. This makes `ClientConfig::new_connection_every` apply to failed
DNS and transport attempts as well as successful connections. A focused unit
test covers the permit lifetime.

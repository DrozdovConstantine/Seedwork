# SeedWork agent plugin

This plugin provides English-language skills for using the SeedWork.Domain,
SeedWork.Application, and SeedWork.Messaging packages, including the Kafka and
RabbitMQ adapters. The skills describe the 0.1.0 library APIs and instruct agents
to verify the versions used by the consuming project. They do not install NuGet
packages. The plugin has no MCP server or external service dependency.

The repository root contains the shared `.claude-plugin/marketplace.json`.
The plugin itself contains a portable `plugin.json` for Codex and a Claude Code
manifest; both clients use the same `skills/` directories. The marketplace uses
Codex's documented Claude-compatible location and a relative plugin source.

## Install from GitHub

These commands require a client version that supports plugins and access to the
repository. Replace `OWNER/REPO` with your repository, for example
`DrozdovConstantine/Seedwork`, after the marketplace and plugin files have been
committed and pushed. Follow the client prompts to enable the plugin.

### Codex

```shell
codex plugin marketplace add OWNER/REPO
```

Open the plugin browser (`/plugins` in Codex), find `seedwork` in
`seedwork-marketplace`, and install it. Start a new session. Marketplace and
plugin support may differ between Codex clients; see the [Codex plugin guide](https://developers.openai.com/plugins/build/plugins).

### Claude Code

```shell
claude plugin marketplace add OWNER/REPO
claude plugin install seedwork@seedwork-marketplace
```

Start a new session and check `claude plugin details seedwork`. Skills can be
invoked explicitly, for example `/seedwork:seedwork-domain`. See the
[Claude Code marketplace guide](https://code.claude.com/docs/en/plugin-marketplaces).

## Update

Maintainers: update the skill instructions alongside library changes and bump
the plugin version in **both** manifests for every released plugin change, so
clients can distinguish cached releases. Keep the two versions equal and publish
the commit to the marketplace's tracked branch or ref.

For Codex, refresh the marketplace and verify the installed plugin in the plugin
browser; restart the client or start a new session if necessary:

```shell
codex plugin marketplace upgrade seedwork-marketplace
```

For Claude Code, refresh the listing and update the installed plugin:

```shell
claude plugin marketplace update seedwork-marketplace
claude plugin update seedwork@seedwork-marketplace
```

Restart or reload the session as required by the client. Refreshing a
marketplace alone does not necessarily update an installed plugin.

## Local validation

From the repository root, with Claude Code installed:

```shell
claude plugin validate ./.seedwork-plugin
claude plugin validate .
claude --plugin-dir ./.seedwork-plugin
```

For a Codex local marketplace, use `codex plugin marketplace add .`, then find
and install `seedwork` in the plugin browser. Local marketplace registration and
installation modify the user's client configuration; remove the test marketplace
afterward if it is no longer needed. On supported clients the root
`.claude-plugin/marketplace.json` is a legacy-compatible marketplace location.

I need to set up AD-group-based access to DB2 LUW 11.5.9 on RHEL — help
me figure out what's actually possible and, if you have shell access,
execute it.

Environment:
- Centrify is already integrated with these RHEL hosts for AD auth
  (already used for AD-group-based OS-level access elsewhere on this
  estate)
- No root on the DB2 servers — only DB2 instance-owner access (dbm
  cfg, GRANT/REVOKE)
- Can install the IBM Security Directory Server (SDS) client myself,
  no root needed
- Goal: DB2 privileges granted to an AD group, not individual accounts

If you have shell access: check whether the AD group already resolves
via `getent group <name>`, check current plugin settings in `db2 get
dbm cfg`, and test whether an AD/Centrify user can already authenticate
without any new plugin.

If not: give me those exact commands and tell me what each possible
result would mean.

Then walk me through the decision tree — when what's already there is
enough, when it's worth asking whoever holds root to install the
Delinea DB2 plugin, and when self-configuring IBM's native LDAP plugin
via the SDS client is the better call — with the concrete DB2 config
and GRANT syntax for whichever path fits. Flag anything specific to
11.5.9 worth checking against current IBM docs, and ask me anything
you need rather than assuming.

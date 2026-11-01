# Render testing deployment

Deploy `server/` as a single Node web service using `render.yaml`.
Keep `SESSION_SECRET` stable and private. Render supplies the listening `PORT`.

The free plan is for testing: it sleeps when idle, and local reports and blocks
are lost on restart or deployment. Matching queues and current calls also reset
on process restart. Do not use multiple instances with this in-memory matcher.

Set the APK's `EXPO_PUBLIC_SIGNALING_URL` to the resulting HTTPS service URL,
then rebuild and install it. The installed APK must be replaced to change its
embedded URL; restarting the backend alone does not change the installed app.

Run `node server/check-live.mjs https://YOUR-SERVICE.onrender.com` to check the
public HTTPS/WebSocket matching, text, and video signaling paths. This check
does not establish camera/audio connectivity.

Default ICE configuration uses STUN. Configure an authorized TURN provider via
`ICE_SERVERS_JSON` for networks where direct peer-to-peer connections fail.
The current endpoint returns configured ICE credentials to clients, so use
testing-only credentials; production requires short-lived TURN credentials.

Do not commit session secrets, `.env` files, or `server/data/` to GitHub.

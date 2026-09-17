# Public Repository Checklist

This POC uses demo-only credentials for a RabbitMQ instance running locally in Docker.

Before publishing or adapting the repository:

- [ ] Confirm the RabbitMQ instance referenced by the code is local/demo-only.
- [ ] Never replace the demo values with production, cloud or shared-environment credentials.
- [ ] Keep `.env`, local secret files and runtime `logs/` ignored.
- [ ] Keep only sanitized sample logs under `docs/sample/`.
- [ ] Confirm `git status` does not show personal API keys, cloud credentials or other secrets.
- [ ] If real credentials were ever committed, remove them from Git history and rotate them.
- [ ] Add repository description and useful topics such as `dotnet`, `csharp`, `rabbitmq`, `microservices`, `messaging`, `distributed-systems`, `dead-letter-queue`, `retry`.

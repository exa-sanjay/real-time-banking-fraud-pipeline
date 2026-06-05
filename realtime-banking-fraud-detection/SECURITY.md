# Security Policy

## Supported Use

This repository is a demo solution for local evaluation and customer walkthroughs. It is not intended to be deployed as a hardened production fraud platform without additional security review.

## Reporting

Do not open public GitHub issues for suspected credential leaks, exposed secrets, or vulnerabilities that could materially affect running environments.

Report security concerns privately to the maintainers of the repository where this solution is hosted.

## Operational Expectations

- Keep real credentials in `.env`, not in committed files.
- Review `.env.example` before adding new variables so placeholders stay non-sensitive.
- Keep BucketFS credentials, database passwords, and model artifacts out of git.
- Exposed demo services are bound to `127.0.0.1` by default and should remain local unless there is a deliberate access requirement.

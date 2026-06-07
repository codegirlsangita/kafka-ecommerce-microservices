# Contributing Guide

## Setup

1. Fork the repository.
2. Clone your fork.
3. Copy `.env.example` to `.env` and adjust values if needed.
4. Install dependencies:
   - `npm run install:all`
5. Start infrastructure:
   - `npm run infra:up`
6. Create Kafka topics:
   - `npm run topics:create`

## Development Workflow

1. Create a branch from `main`.
2. Make focused changes.
3. Verify syntax:
   - `npm run check:syntax`
4. Run and test the workflow:
   - `node scripts/test-workflow.js`
5. Open a pull request with:
   - problem statement
   - change summary
   - test evidence (logs or screenshots)

## Code Standards

- Keep service boundaries explicit.
- Prefer environment variables over hardcoded values.
- Add or update docs when behavior changes.
- Preserve backward compatibility for topic names unless migration is documented.

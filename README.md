# Airflow-superset-dev

A local development environment combining Apache Airflow and Apache Superset using Docker Compose.

This repository provides a lightweight workspace for developing and testing Airflow DAGs alongside Superset for data visualization.

Contents
--------

- `docker-compose.yaml` — service definitions
-------------

- Docker (v20.10+)
- Docker Compose (v2+ or `docker compose` plugin)

Quickstart
----------

1. Build the images (first run or after changes):

```bash
docker compose build
```

2. Start the environment in the background:

```bash
docker compose up 
```

3. View running services:

```bash
docker compose ps
```

4. Follow logs (example):

```bash
docker compose logs -f
```

Default service ports
---------------------

- Superset: 8088 (confirm in `docker-compose.yaml`)

---------------

- If a service fails to start, inspect logs via `docker compose logs <service>`.
- If ports are in use, check `docker compose ps` and your local processes.

Contributing
------------

Feel free to open issues or submit pull requests with improvements to the compose configuration, helpful scripts, or example DAGs.

License
-------

This repository does not specify a license. Add a `LICENSE` file if you want to make the project's license explicit.

Contact
-------

For questions about this workspace setup, open an issue in this repository.

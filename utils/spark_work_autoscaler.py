import docker
from logger import log, log_step
from config import DOCKER_NETWORK, SPARK_AUTOSCALE, SPARK_MASTER, SPARK_WORKER_CONTAINER_PREFIX, SPARK_WORKER_CPU_THRESHOLD, SPARK_WORKER_IMAGE, SPARK_WORKER_MAX, SPARK_WORKER_MIN, create_docker_client
try:
    client = create_docker_client()
    client.ping()
except Exception as exc:
    log.error(f'Docker not available, disabling Spark autoscaling: {exc}')
    client = None

def _list_workers():
    if client is None:
        return []
    try:
        containers = client.containers.list()
        return [c for c in containers if c.name.startswith(SPARK_WORKER_CONTAINER_PREFIX)]
    except Exception as exc:
        log.error(f'Failed to list worker containers: {exc}')
        return []

def _start_worker(name: str):
    if client is None:
        return
    with log_step(f"Start Spark worker '{name}'"):
        try:
            try:
                existing = client.containers.get(name)
                if existing.status in ('exited', 'dead', 'created'):
                    existing.remove()
                    log.info(f"Removed stale container '{name}'")
                else:
                    log.warning(f"Container '{name}' already running")
                    return
            except docker.errors.NotFound:
                pass
            env = {'SPARK_MODE': 'worker', 'SPARK_MASTER_URL': SPARK_MASTER}
            client.containers.run(SPARK_WORKER_IMAGE, name=name, detach=True, network=DOCKER_NETWORK, environment=env)
            log.info(f"Started Spark worker container '{name}'")
        except Exception as exc:
            log.error(f"Failed to start Spark worker '{name}': {exc}")

def _cpu_percent(container) -> float:
    if client is None:
        return 0.0
    try:
        stats = container.stats(stream=False)
        cpu_delta = stats['cpu_stats']['cpu_usage']['total_usage'] - stats['precpu_stats']['cpu_usage']['total_usage']
        system_delta = stats['cpu_stats']['system_cpu_usage'] - stats['precpu_stats']['system_cpu_usage']
        if system_delta > 0:
            cpu_count = len(stats['cpu_stats']['cpu_usage'].get('percpu_usage', []))
            return cpu_delta / system_delta * cpu_count * 100.0
    except Exception as exc:
        log.error(f'Failed to read stats for {container.name}: {exc}')
        return 0.0

def check_and_scale_workers() -> None:
    if not SPARK_AUTOSCALE or client is None:
        return
    with log_step('Check and scale Spark workers'):
        workers = _list_workers()
        count = len(workers)
        if count < SPARK_WORKER_MIN:
            for i in range(count, SPARK_WORKER_MIN):
                name = f'{SPARK_WORKER_CONTAINER_PREFIX}{i + 1}'
                _start_worker(name)
            return
        avg_cpu = 0.0
        if workers:
            percentages = [_cpu_percent(w) for w in workers]
            avg_cpu = sum(percentages) / len(percentages)
        if avg_cpu < SPARK_WORKER_CPU_THRESHOLD:
            return
        if count >= SPARK_WORKER_MAX:
            log.info(f'CPU {avg_cpu:.1f}% exceeds threshold but already at max workers ({count})')
            return
        name = f'{SPARK_WORKER_CONTAINER_PREFIX}{count + 1}'
        _start_worker(name)

def _stop_worker(container) -> None:
    if client is None:
        return
    with log_step(f"Stop Spark worker '{container.name}'"):
        try:
            container.stop(timeout=10)
            container.remove()
            log.info(f"Stopped Spark worker container '{container.name}'")
        except Exception as exc:
            log.error(f'Failed to stop worker {container.name}: {exc}')

def cleanup_workers() -> None:
    if client is None:
        return
    with log_step('Cleanup Spark workers'):
        workers = _list_workers()
        for w in workers:
            _stop_worker(w)

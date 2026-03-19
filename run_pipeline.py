import subprocess
import sys
import time
from pathlib import Path
from collections import defaultdict, deque

PROJECT_ROOT = Path(__file__).resolve().parent


class Task:
    def __init__(self, name, script_path, depends_on=None):
        self.name = name
        self.script_path = script_path
        self.depends_on = depends_on or []


TASKS = {
    "ingest_data": Task(
        name="Ingest raw data",
        script_path=PROJECT_ROOT / "src" / "ingest_data.py",
    ),
    "prepare_features": Task(
        name="Prepare features",
        script_path=PROJECT_ROOT / "src" / "prepare_features.py",
        depends_on=["ingest_data"],
    ),
    "validate_staging": Task(
        name="Validate staging",
        script_path=PROJECT_ROOT / "src" / "validate_staging.py",
        depends_on=["prepare_features"],
    ),
    "build_star_schema": Task(
        name="Build star schema",
        script_path=PROJECT_ROOT / "src" / "build_star_schema.py",
        depends_on=["validate_staging"],
    ),
    "build_mart_pet_outcome_summary": Task(
        name="Build mart.pet_outcome_summary",
        script_path=PROJECT_ROOT / "src" / "build_mart_pet_outcome_summary.py",
        depends_on=["build_star_schema"],
    ),
    "build_mart_breed_adoption_summary": Task(
        name="Build mart.breed_adoption_summary",
        script_path=PROJECT_ROOT / "src" / "build_mart_breed_adoption_summary.py",
        depends_on=["build_star_schema"],
    ),
    "build_ml_dataset": Task(
        name="Build ml.adoption_training_data",
        script_path=PROJECT_ROOT / "src" / "build_ml_dataset.py",
        depends_on=["build_star_schema"],
    ),
}


def topological_sort(tasks: dict[str, Task]) -> list[str]:
    in_degree = {task_id: 0 for task_id in tasks}
    graph = defaultdict(list)

    for task_id, task in tasks.items():
        for dependency in task.depends_on:
            graph[dependency].append(task_id)
            in_degree[task_id] += 1

    queue = deque([task_id for task_id, degree in in_degree.items() if degree == 0])
    ordered = []

    while queue:
        current = queue.popleft()
        ordered.append(current)

        for downstream in graph[current]:
            in_degree[downstream] -= 1
            if in_degree[downstream] == 0:
                queue.append(downstream)

    if len(ordered) != len(tasks):
        raise ValueError("Cycle detected in pipeline DAG.")

    return ordered


def run_task(task: Task) -> None:
    start = time.time()

    print("\n" + "=" * 70)
    print(f"RUNNING: {task.name}")
    print(f"SCRIPT:  {task.script_path}")
    print("=" * 70)

    result = subprocess.run([sys.executable, str(task.script_path)])

    if result.returncode != 0:
        elapsed = round(time.time() - start, 2)
        print(f"\nFAILED: {task.name} after {elapsed} sec")
        sys.exit(result.returncode)

    elapsed = round(time.time() - start, 2)
    print(f"COMPLETED: {task.name} ({elapsed} sec)")


def main() -> None:
    print("Starting pet adoption DAG-style pipeline...")

    execution_order = topological_sort(TASKS)

    print("\nExecution order:")
    for i, task_id in enumerate(execution_order, start=1):
        print(f"{i}. {TASKS[task_id].name}")

    for task_id in execution_order:
        run_task(TASKS[task_id])

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
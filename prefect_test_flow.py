import httpx
from datetime import timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def get_url(url: str, params: dict = None):
    response = httpx.get(url, params=params)
    response.raise_for_status()
    return response.json()


def get_open_issues(repo_name: str, open_issues_count: int, per_page: int = 100):
    issues = []
    pages = range(1, -(open_issues_count // -per_page) + 1)
    for page in pages:
        issues.append(
            get_url(
                f"https://api.github.com/repos/{repo_name}/issues",
                params={"page": page, "per_page": per_page, "state": "open"},
            )
        )
    return [i for p in issues for i in p]


@flow
def get_repo_info(
    repo_name: str = "PrefectHQ/prefect", retries=3, retry_delay_seconds=5
):
    repo = get_url(f"https://api.github.com/repos/{repo_name}")
    issues = get_open_issues(repo_name, repo["open_issues_count"])
    issues_per_user = len(issues) / len(set([i["user"]["id"] for i in issues]))
    logger = get_run_logger()
    logger.info(f"PrefectHQ/prefect repository statistics 🤓:")
    logger.info(f"Stars 🌠 : {repo['stargazers_count']}")
    logger.info(f"Forks 🍴 : {repo['forks_count']}")
    logger.info(f"Average open issues per user 💌 : {issues_per_user:.2f}")


if __name__ == "__main__":
    get_repo_info()

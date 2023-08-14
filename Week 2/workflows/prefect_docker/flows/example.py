import httpx
from prefect import flow, get_run_logger

@flow
def get_repo_info():
    logger = get_run_logger()
    url = "https://api.github.com/repos/PrefectHQ/prefect"
    response = httpx.get(url)
    response.raise_for_status()
    repo = response.json()
    print(f"PrefectHQ/prefect repository statistics 🤓:")
    print(f"Stars 🌠 : {repo['stargazers_count']}")
    print(f"Forks 🍴 : {repo['forks_count']}")
    logger.info(f"PrefectHQ/prefect repository statistics 🤓:")
    logger.info(f"Stars 🌠 : {repo['stargazers_count']}")
    logger.info(f"Forks 🍴 : {repo['forks_count']}")


if __name__ == "__main__":
    get_repo_info()
import datetime
import os
import shutil
import requests
import git
from pydantic import BaseModel, DirectoryPath, Field, FilePath, HttpUrl
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret


@task(tags=["github"])
def create_pr(title, branch, token, repo, base):
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
    }
    url = f"https://api.github.com/repos/{repo}/pulls"
    form = {"title": title, "head": branch, "base": base}
    res = requests.post(url, headers=headers, json=form)
    return res


@task(tags=["github"])
def push_data_to_repo(
    file_path, branch, repo, username, password, commit_message, file_path_git
):
    """creates a feature branch adds the created graph file and pushes the branch

    Args:
        file_path (path): path of the file to push
        branch (str, optional): Name of the feature branch to use, suto-generated if None. Defaults to None.
    """

    logger = get_run_logger()
    full_local_path = os.path.join(os.getcwd(), "source-data")
    remote = f"https://{username}:{password}@github.com/{repo}.git"
    logger.info(f"Cloning {remote} to {full_local_path}")
    repo = git.Repo.clone_from(remote, full_local_path)
    repo.git.checkout("-b", branch)
    os.makedirs(
        os.path.dirname(os.path.join(full_local_path, *file_path_git.split("/"))),
        exist_ok=True,
    )
    shutil.copyfile(file_path, os.path.join(full_local_path, *file_path_git.split("/")))
    repo.git.add(file_path_git)
    repo.index.commit(commit_message)
    origin = repo.remote(name="origin")
    origin.push(refspec=f"{branch}:{branch}")
    return True


class Params(BaseModel):
    repo: str = Field(
        "intavia/source-data",
        description="GitHub Repository in the format 'OWNER/REPO'",
    )
    branch_name: str = Field(
        ...,
        description="Branch name to use, auto-generated if not set",
    )
    branch_name_add_date: bool = Field(
        False, description="Whether to add the current date to the branch name"
    )
    username_secret: str = Field(
        "github-username",
        description="Name of the prefect secret that contains the username",
    )
    password_secret: str = Field(
        "github-password",
        description="Name of the prefect secret that contains the password. Use tokens instead of your personal password.",
    )
    file_path: FilePath = Field(..., description="Path of the file to ingest")
    file_path_git: str = Field(
        "datasets/apis_data.ttl",
        alias="File Path Git",
        description="Path of the file to use within the Git repo",
    )
    named_graphs_used: list[HttpUrl] = Field(
        None,
        description="Named graphs used in the generation of the data",
    )
    commit_message: str = Field(
        "Updates data to latest",
        alias="Commit Message",
        description="Message used for the commit.",
    )
    auto_pr: bool = Field(True, description="Wheter to automatically add a PR")
    base_branch_pr: str = Field(
        "main",
        description="Base branch for creating the PR against",
    )


@flow()
def push_data_to_repo_flow(params: Params):
    username = Secret.load(params.username_secret).get()
    password = Secret.load(params.password_secret).get()
    if params.branch_name_add_date:
        branch_name = f"{params.branch_name}-{datetime.now().strftime('%d-%m-%Y')}"
    else:
        branch_name = params.branch_name
    commit_message = f"feat: {params.commit_message}"
    res = push_data_to_repo(
        params.file_path,
        branch_name,
        params.repo,
        username,
        password,
        commit_message,
        params.file_path_git,
    )
    if res and params.auto_pr:
        create_pr(
            f"add new data from {branch_name}",
            branch_name,
            password,
            params.repo,
            params.base_branch_pr,
        )

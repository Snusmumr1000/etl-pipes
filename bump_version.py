# type: ignore
# ruff: noqa
import subprocess

import toml
import typer

app = typer.Typer()


def bump_version(current_version: str, major: bool, minor: bool, patch: bool) -> str:
    major_version, minor_version, patch_version = current_version.split(".")
    if major:
        major_version = str(int(major_version) + 1)
        minor_version = "0"
        patch_version = "0"
    elif minor:
        minor_version = str(int(minor_version) + 1)
        patch_version = "0"
    elif patch:
        patch_version = str(int(patch_version) + 1)
    return ".".join([major_version, minor_version, patch_version])


def run_command(command: list[str]):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise Exception(stderr.decode("utf-8"))
    return stdout.decode("utf-8")


@app.command()
def main(
    major: bool = typer.Option(False, "--major"),
    minor: bool = typer.Option(False, "--minor"),
    patch: bool = typer.Option(False, "--patch"),
):
    if sum([major, minor, patch]) != 1:
        raise typer.BadParameter(
            "Exactly one of --major, --minor, or --patch must be provided."
        )

    new_version = change_pyproject_version(major, minor, patch)

    # Poetry build and publishw
    poetry_build()
    poetry_publish()

    # Git commit
    git_commit_new_version(new_version)
    git_tag_new_version(new_version)
    git_push_new_version()

    typer.echo(f"Version bumped to {new_version} and published.")


def change_pyproject_version(major, minor, patch):
    # Load pyproject.toml
    with open("pyproject.toml") as file:
        pyproject = toml.load(file)
    current_version = pyproject["tool"]["poetry"]["version"]
    new_version = bump_version(current_version, major, minor, patch)
    # Update pyproject.toml with new version
    typer.echo(f"Bumping version from {current_version} to {new_version}")
    pyproject["tool"]["poetry"]["version"] = new_version
    with open("pyproject.toml", "w") as file:
        toml.dump(pyproject, file)
    return new_version


def git_push_new_version():
    typer.echo("Pushing changes...")
    run_command(["git", "push"])


def git_tag_new_version(new_version):
    run_command(["git", "tag", new_version])


def git_commit_new_version(new_version):
    typer.echo("Committing changes...")
    run_command(["git", "add", "pyproject.toml"])
    run_command(["git", "commit", "-m", f"Bump version to {new_version}"])


def poetry_publish():
    typer.echo("Publishing package...")
    run_command(["poetry", "publish"])


def poetry_build():
    typer.echo("Building package...")
    run_command(["poetry", "build"])


if __name__ == "__main__":
    app()

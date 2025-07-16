from pathlib import Path


def get_project_root() -> Path:
    """
    Retourne le chemin absolu du dossier racine du projet (le dossier parent du dossier utils).
    """
    return Path(__file__).parent.parent.resolve()


def make_dir(path: Path) -> None:
    """
    Crée le dossier donné par 'path' s'il n'existe pas, avec tous ses parents.
    """
    if not path.exists():
        path.mkdir(parents=True)

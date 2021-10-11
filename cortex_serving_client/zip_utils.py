import shutil
import zipfile
from pathlib import Path


def zip_dir(dir_path, archive_path: str):
    """
    Zips contents of a directory without the root directory.
    Example usage:
    zip_dir('dummy_dir', 'code.zip')
    will archive contents of directory 'dummy_dir' into a file 'code.zip' in CWD
    :param dir_path: Path to directory to compress.
    :param archive_path: Path to the zip file.
    """
    archive_path = str(archive_path)
    if archive_path.endswith('.zip'):
        archive_path = archive_path[:-4]  # remove .zip
    else:
        raise RuntimeError(f"archive_path {archive_path} must end with '.zip'!")

    shutil.make_archive(archive_path, 'zip', root_dir=dir_path, base_dir='.')


def add_file_to_zip(archive_path, file_path):
    with zipfile.ZipFile(archive_path, "a") as zf:
        zf.write(file_path, Path(file_path).name)

import os
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
    with zipfile.ZipFile(archive_path, "w") as zf:
        cwd = Path().cwd()
        os.chdir(str(dir_path))
        for dirname, subdirs, files in os.walk('.'):
            if dirname != '.':
                zf.write(dirname)
            for filename in files:
                zf.write(os.path.join(dirname, filename))
        os.chdir(cwd)


def add_file_to_zip(archive_path, file_path):
    with zipfile.ZipFile(archive_path, "a") as zf:
        zf.write(file_path, Path(file_path).name)
    print(f"Added {file_path} to archive {archive_path}")
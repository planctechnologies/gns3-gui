from contextlib import contextmanager
import io
import json
from socket import error as socket_error
import logging
import os
import select
import tempfile
import time
import zipfile

from PyQt4.QtCore import QThread
from PyQt4.QtCore import pyqtSignal

from .exceptions import KeyPairExists
from .rackspace_ctrl import RackspaceCtrl, get_provider
from ..topology import Topology
from ..servers import Servers

log = logging.getLogger(__name__)


@contextmanager
def ssh_client(host, key_string):
    """
    Context manager wrapping a SSHClient instance: the client connects on
    enter and close the connection on exit
    """

    import paramiko
    class AllowAndForgetPolicy(paramiko.MissingHostKeyPolicy):
        """
        Custom policy for server host keys: we simply accept the key
        the server sent to us without storing it.
        """
        def missing_host_key(self, *args, **kwargs):
            """
            According to MissingHostKeyPolicy protocol, to accept
            the key, simply return.
            """
            return

    client = paramiko.SSHClient()
    try:
        f_key = io.StringIO(key_string)
        key = paramiko.RSAKey.from_private_key(f_key)
        client.set_missing_host_key_policy(AllowAndForgetPolicy())
        client.connect(hostname=host, username="root", pkey=key)
        yield client
    except socket_error as e:
        log.debug("SSH connection socket error to {}: {}".format(host, e))
        yield None
    except Exception as e:
        log.debug("SSH connection error to {}: {}".format(host, e))
        yield None
    finally:
        client.close()


class ListInstancesThread(QThread):
    """
    Helper class to retrieve data from the provider in a separate thread,
    avoid freezing the gui
    """
    haveInstanceInfo = pyqtSignal(object)

    def __init__(self, parent, provider):
        super(QThread, self).__init__(parent)
        self._provider = provider

    def run(self):
        try:
            instances = self._provider.list_instances()
            log.debug('Instance list: {}'.format([(i.name, i.state) for i in instances]))
            self.haveInstanceInfo.emit(instances)
        except Exception as e:
            log.info('list_instances error: {}'.format(e))


class DeleteInstanceThread(QThread):
    """
    Helper class to remove an instance in a separate thread
    """
    instanceDeleted = pyqtSignal(object)

    def __init__(self, parent, provider, instance):
        super(QThread, self).__init__(parent)
        self._provider = provider
        self._instance = instance

    def run(self):
        if self._provider.delete_instance(self._instance):
            self.instanceDeleted.emit(self._instance)


class UploadProjectThread(QThread):
    """
    Zip and Upload project to the cloud
    """

    # signals to update the progress dialog.
    error = pyqtSignal(str, bool)
    completed = pyqtSignal()
    update = pyqtSignal(int)

    def __init__(self, parent, cloud_settings, project_path, images_path):
        super(QThread, self).__init__(parent)
        self.cloud_settings = cloud_settings
        self.project_path = project_path
        self.images_path = images_path

    def run(self):
        try:
            log.info("Exporting project to cloud")
            self.update.emit(0)

            zipped_project_file = self.zip_project_dir()

            self.update.emit(10)  # update progress to 10%

            provider = get_provider(self.cloud_settings)
            provider.upload_file(zipped_project_file, 'projects/' + os.path.basename(zipped_project_file))

            self.update.emit(20)  # update progress to 20%

            topology = Topology.instance()
            images = set([node.settings()["image"] for node in topology.nodes() if 'image' in node.settings()])

            for i, image in enumerate(images):
                provider.upload_file(image, 'images/' + os.path.relpath(image, self.images_path))
                self.update.emit(20 + (float(i) / len(images) * 80))

            self.completed.emit()
        except Exception as e:
            log.exception("Error exporting project to cloud")
            self.error.emit("Error exporting project: {}".format(str(e)), True)

    def zip_project_dir(self):
        """
        Zips project files
        :return: path to zipped project file
        """
        project_name = os.path.basename(self.project_path)
        output_filename = os.path.join(tempfile.gettempdir(), project_name + ".zip")
        project_dir = os.path.dirname(self.project_path)
        relroot = os.path.abspath(os.path.join(project_dir, os.pardir))
        with zipfile.ZipFile(output_filename, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for root, dirs, files in os.walk(project_dir):
                # add directory (needed for empty dirs)
                zip_file.write(root, os.path.relpath(root, relroot))
                for file in files:
                    filename = os.path.join(root, file)
                    if os.path.isfile(filename) and not self._should_exclude(filename):  # regular files only
                        arcname = os.path.join(os.path.relpath(root, relroot), file)
                        zip_file.write(filename, arcname)

        return output_filename

    def _should_exclude(self, filename):
        """
        Returns True if file should be excluded from zip of project files
        :param filename:
        :return: True if file should be excluded from zip, False otherwise
        """
        return filename.endswith('.ghost')

    def stop(self):
        self.quit()


class UploadFilesThread(QThread):
    """
    Uploads files to cloud files

    :param cloud_settings:
    :param files_to_upload: list of tuples of (file path, file name to save in cloud)
    """

    error = pyqtSignal(str, bool)
    completed = pyqtSignal()
    update = pyqtSignal(int)

    def __init__(self, parent, cloud_settings, files_to_upload):
        super(QThread, self).__init__(parent)
        self._cloud_settings = cloud_settings
        self._files_to_upload = files_to_upload

    def run(self):
        self.update.emit(0)

        try:
            for i, file_to_upload in enumerate(self._files_to_upload):
                provider = get_provider(self._cloud_settings)

                log.debug('Uploading image {} to cloud as {}'.format(file_to_upload[0], file_to_upload[1]))
                provider.upload_file(file_to_upload[0], file_to_upload[1])

                self.update.emit((i+1) * 100 / len(self._files_to_upload))
                log.debug('Uploading image completed')
        except Exception as e:
            log.exception("Error uploading images to cloud")
            self.error.emit("Error uploading images: {}".format(str(e)), True)

        self.completed.emit()

    def stop(self):
        self.quit()


class DownloadProjectThread(QThread):
    """
    Downloads project from cloud storage
    """

    # signals to update the progress dialog.
    error = pyqtSignal(str, bool)
    completed = pyqtSignal()
    update = pyqtSignal(int)

    def __init__(self, parent, cloud_project_file_name, project_dest_path, images_dest_path, cloud_settings):
        super(QThread, self).__init__(parent)
        self.project_name = cloud_project_file_name
        self.project_dest_path = project_dest_path
        self.images_dest_path = images_dest_path
        self.cloud_settings = cloud_settings

    def run(self):
        try:
            self.update.emit(0)
            provider = get_provider(self.cloud_settings)
            zip_file = provider.download_file(self.project_name)
            zip_file = zipfile.ZipFile(zip_file, mode='r')
            zip_file.extractall(self.project_dest_path)
            zip_file.close()
            project_name = zip_file.namelist()[0].strip('/')

            self.update.emit(20)

            with open(os.path.join(self.project_dest_path, project_name, project_name + '.gns3'), 'r') as f:
                project_settings = json.loads(f.read())

                images = set()
                for node in project_settings["topology"].get("nodes", []):
                    if "properties" in node and "image" in node["properties"]:
                        images.add(node["properties"]["image"])

            image_names_in_cloud = provider.find_storage_image_names(images)

            for i, image in enumerate(images):
                dest_path = os.path.join(self.images_dest_path, *image_names_in_cloud[image].split('/')[1:])

                if not os.path.exists(os.path.dirname(dest_path)):
                    os.makedirs(os.path.dirname(dest_path))

                provider.download_file(image_names_in_cloud[image], dest_path)
                self.update.emit(20 + (float(i) / len(images) * 80))

            self.completed.emit()
        except Exception as e:
            log.exception("Error importing project from cloud")
            self.error.emit("Error importing project: {}".format(str(e)), True)

    def stop(self):
        self.quit()


class DeleteProjectThread(QThread):
    """
    Deletes project from cloud storage
    """

    # signals to update the progress dialog.
    error = pyqtSignal(str, bool)
    completed = pyqtSignal()
    update = pyqtSignal(int)

    def __init__(self, parent, project_file_name, cloud_settings):
        super(QThread, self).__init__(parent)
        self.project_file_name = project_file_name
        self.cloud_settings = cloud_settings

    def run(self):
        try:
            provider = get_provider(self.cloud_settings)
            provider.delete_file(self.project_file_name)
            self.completed.emit()
        except Exception as e:
            log.exception("Error deleting project")
            self.error.emit("Error deleting project: {}".format(str(e)), True)

    def stop(self):
        pass


def get_cloud_projects(cloud_settings):
    provider = get_provider(cloud_settings)
    return provider.list_projects()

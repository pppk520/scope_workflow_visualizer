import time
import os
import stat
import codecs

class FileUtility(object):

    @staticmethod
    def get_file_age_seconds(filepath):
        if not os.path.exists(filepath):
            return None

        return time.time() - os.stat(filepath)[stat.ST_MTIME]

    @staticmethod
    def mkdir_p(the_folder):
        try:
            os.makedirs(the_folder)
        except Exception as ex:
            pass

    @staticmethod
    def delete_file(filepath):
        os.remove(filepath)

    @staticmethod
    def list_files_recursive(root_folder, target_suffix=None):
        filelist = []

        for root, directories, filenames in os.walk(root_folder):
            for filename in filenames:
                if not target_suffix or filename.endswith(target_suffix):
                    filelist.append(os.path.join(root, filename).replace('\\', '/'))

        return filelist

    @staticmethod
    def get_file_content(filepath):
        with open(filepath) as f:
            return f.read()


if __name__ == '__main__':
    print(FileUtility.get_file_age_seconds(__file__))

    for f in FileUtility.list_files_recursive("D:/workspace/AdInsights/private/Backend/SOV", target_suffix='.config'):
        print(f)
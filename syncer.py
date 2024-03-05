from dirsync import sync
source_path = r'C:\Users\bmuise\PycharmProjects\2024SpeDataScience'
target_path = r'G:\My Drive\SpeDataScience'

sync(source_path, target_path, 'sync', twoway=True, purge=True)  # for syncing two ways

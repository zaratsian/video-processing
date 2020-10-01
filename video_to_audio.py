
import sys,re
import subprocess

video_file = sys.argv[1]
audio_file = '{}.wav'.format(re.sub('\.[a-zA-Z0-9]{3,4}','',video_file))

print(f'''[ INFO ] Copying file {video_file} from GCP''')
command1 = f'''gsutil cp "gs://video-processing-dropzone/{video_file}" .'''
subprocess.call(command1, shell=True)

print('[ INFO ] Extracting Audio from Video file')
command2 = f'''ffmpeg -i "{video_file}" -ss 00:00:00 -to 00:02:00 -async 1 "{audio_file}"'''
subprocess.call(command2, shell=True)

print('[ INFO ] Removing video file')
command3 = f'''rm "{video_file}"'''
subprocess.call(command3, shell=True)

print('[ INFO ] Complete!')


#ZEND



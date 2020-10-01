
import sys
from google.cloud import speech_v1p1beta1 as speech

client = speech.SpeechClient()

speech_file = sys.argv[1]

with open(speech_file, "rb") as audio_file:
    content = audio_file.read()

audio = speech.RecognitionAudio(content=content)

config = speech.RecognitionConfig(
    encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
    sample_rate_hertz=8000,
    language_code="en-US",
    enable_speaker_diarization=True,
    diarization_speaker_count=2,
)

operation = client.long_running_recognize(
    request={"config": config, "audio": audio}
)

print("Waiting for operation to complete...")
response = operation.result(timeout=90)

for result in response.results:
    # The first alternative is the most likely one for this portion.
    print(u"Transcript: {}".format(result.alternatives[0].transcript))
    print("Confidence: {}".format(result.alternatives[0].confidence))

#ZEND

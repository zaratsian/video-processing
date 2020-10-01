
from flask import Flask, render_template, json, request, redirect, jsonify, url_for, session
from werkzeug import secure_filename
import os,sys,re
import json
import requests
import datetime,time
import html
#import six
import base64
#import urllib.parse
from google.cloud import storage,bigquery
from pytube import YouTube

from google.cloud.storage.blob import Blob
from google.cloud import language_v1
from google.cloud.language_v1 import enums

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'zproject201807-04ed54d70ba2.json'


############################################################
#
#   Flask App
#
############################################################


app = Flask(__name__)
app.secret_key = os.urandom(24)


############################################################
#
#   Functions
#
############################################################

def download_media_to_gcs(media_url): 
    print('[ MEDIA URL ] {}'.format( media_url ))    
    project_id = 'zproject201807'
    gcs_bucket = 'video-processing-dropzone'
    
    url     = 'https://us-central1-zproject201807.cloudfunctions.net/download_youtube_clip'.format(project_id)
    headers = {'Content-Type':'application/json'}
    payload = {'youtube_url':media_url, 'bucket_name':gcs_bucket}
    print('[ INFO ] Requesting URL: {}'.format(url))
    r = requests.post(url, headers=headers, data=json.dumps(payload))
    print('[ INFO ] Status Code: {}'.format(r.status_code))
    if r.status_code == 200:
        return r.status_code, r.content.decode('utf-8')
    else:
        print('[ ERROR ] Status Code {}. Failed to download media URL. {}'.format( r.status_code, r.content ))
        return r.status_code, r.content.decode('utf-8')


def upload_file_to_gcs( video_file ):
    project_id = 'zproject201807'
    gcs_bucket = 'video-processing-dropzone'
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket( gcs_bucket )
        blob = bucket.blob( video_file.filename )
        blob.upload_from_string( video_file.read() ) #, content_type='video/mp4')
        return True
    except:
        print('[ ERROR ] Failed at upload_file_to_gcs. Could not upload {}'.format(video_file))
        return False


def bq_query_table(query):
    ''' BigQuery Query Table '''
    
    bigquery_client = bigquery.Client()
    
    #query = ('''select * from video_analysis1.video_metadata1 limit 5''')
    
    query_job = bigquery_client.query(
        query,
        # Location must match that of the dataset(s) referenced in the query.
        location='US')  # API request - starts the query
    
    data = [row for row in query_job]
    return data


def gcp_storage_download_as_string(bucket_name, blob_name):
    '''
        Downloads a blob from the bucket, and outputs as a string.
    '''
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob_content = blob.download_as_string()
        
        return blob_content
    
    except Exception as e:
        print('[ ERROR ] {}'.format(e))



def sample_analyze_sentiment(text_content):
    """
    Analyzing Sentiment in a String

    Args:
      text_content The text content to analyze
    """
    client = language_v1.LanguageServiceClient()
    
    # Available types: PLAIN_TEXT, HTML
    type_ = enums.Document.Type.PLAIN_TEXT
    
    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "en"
    document = {"content": text_content, "type": type_, "language": language}
    
    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = enums.EncodingType.UTF8
    
    response = client.analyze_sentiment(document, encoding_type=encoding_type)
    
    # Get overall sentiment of the input document
    doc_sentiment = response.document_sentiment.score
    doc_sentiment_magnitude = response.document_sentiment.magnitude
    
    print(u"Document sentiment score:     {}".format(doc_sentiment))
    print(u"Document sentiment magnitude: {}".format(doc_sentiment_magnitude))
    
    results = []
    # Get sentiment for all sentences in the document
    for sentence in response.sentences:
        sentence_text = sentence.text.content
        sentence_text_sentiment = sentence.sentiment.score
        sentence_text_sentiment_magnitude = sentence.sentiment.magnitude
        
        json_payload = {
            "sentence":     sentence_text,
            "sentiment":    sentence_text_sentiment,
            "magnitude":    sentence_text_sentiment_magnitude
        }
        results.append(json_payload)
        
        #print(json.dumps(json_payload, indent=4))
        
        #print(u"Sentence text: {}".format(sentence_text))
        #print(u"Sentence sentiment score: {}".format(sentence_text_sentiment))
        #print(u"Sentence sentiment magnitude: {}".format(sentence_text_sentiment_magnitude))
    
    print(json.dumps(results, indent=4))
    # Get the language of the text, which will be the same as
    # the language specified in the request or, if not specified,
    # the automatically-detected language.
    #print(u"Language of the text: {}".format(response.language))
    return doc_sentiment, doc_sentiment_magnitude, results



def sample_analyze_entity_sentiment(text_content):
    """
    Analyzing Entity Sentiment in a String

    Args:
      text_content The text content to analyze
    """
    client = language_v1.LanguageServiceClient()
    
    # Available types: PLAIN_TEXT, HTML
    type_ = enums.Document.Type.PLAIN_TEXT
    
    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "en"
    document = {"content": text_content, "type": type_, "language": language}
    
    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = enums.EncodingType.UTF8
    
    response = client.analyze_entity_sentiment(document, encoding_type=encoding_type)
    # Loop through entitites returned from the API
    entity_results = []
    for entity in response.entities:
        
        entity_name = entity.name
        entity_type = enums.Entity.Type(entity.type).name
        entity_salience = entity.salience
        entity_sentiment = entity.sentiment.score
        entity_sentiment_magnitude = entity.sentiment.magnitude
        
        json_payload = {
            "entity":entity_name,
            "entity_type":entity_type,
            "entity_salience":entity_salience,
            "sentiment":entity_sentiment,
            "sentiment_magnitude":entity_sentiment_magnitude
        }
        
        entity_results.append(json_payload)
        
        print(u"Entity name: {}".format(entity_name))
        # Get entity type, e.g. PERSON, LOCATION, ADDRESS, NUMBER, et al
        print(u"Entity type: {}".format(entity_type))
        # Get the salience score associated with the entity in the [0, 1.0] range
        print(u"Salience score: {}".format(entity_salience))
        # Get the aggregate sentiment expressed for this entity in the provided document.
        #sentiment = entity.sentiment
        print(u"Entity sentiment score: {}".format(entity_sentiment))
        print(u"Entity sentiment magnitude: {}".format(entity_sentiment_magnitude))
        # Loop over the metadata associated with entity. For many known entities,
        # the metadata is a Wikipedia URL (wikipedia_url) and Knowledge Graph MID (mid).
        # Some entity types may have additional metadata, e.g. ADDRESS entities
        # may have metadata for the address street_name, postal_code, et al.
        #for metadata_name, metadata_value in entity.metadata.items():
        #    print(u"{} = {}".format(metadata_name, metadata_value))
        
        # Loop over the mentions of this entity in the input document.
        # The API currently supports proper noun mentions.
        #for mention in entity.mentions:
        #    print(u"Mention text: {}".format(mention.text.content))
        #    # Get the mention type, e.g. PROPER for proper noun
        #    print(u"Mention type: {}".format(enums.EntityMention.Type(mention.type).name))
    
    # Get the language of the text, which will be the same as
    # the language specified in the request or, if not specified,
    # the automatically-detected language.
    #print(u"Language of the text: {}".format(response.language))
    print(json.dumps(entity_results, indent=4))
    return entity_results


############################################################
#
#   Home
#
############################################################
@app.route('/', methods = ['GET','POST'])
#@flask_login.login_required
def index():
    
    try:
        user = request.headers['X-Goog-Authenticated-User-Email'].split(':')[-1] 
    except:
        user = 'Dan Z'
    
    if request.method == 'GET':
        return render_template('index.html', user=user, message='', youtube_title='')
    
    if request.method == 'POST':
        
        media_url = request.form['media_url']
        status_code, content = download_media_to_gcs(media_url)
        
        if status_code == 200:
            youtube_title = content
            message = 'Successfully saved media to Google Cloud Storage'
        else:
            youtube_title = ''
            message = 'Warning - Did not process media URL. Status Code: {}'.format(status_code)
        
        return render_template('index.html', user=user, status_code=status_code, message=message, youtube_title=youtube_title)


@app.route('/upload_file', methods=['GET','POST'])
def upload_file():
    if request.method == 'POST':
        video_file = request.files['video_file']
        response   = upload_file_to_gcs( video_file )
        if response:
            return redirect( url_for('index', message='File Uploaded') )
        else:
            return redirect( url_for('index', message='Error: File not uploaded') )


############################################################
#
#   Media Library
#
############################################################
@app.route('/media', methods = ['GET','POST'])
#@flask_login.login_required
def get_media():
    try:
        user = request.headers['X-Goog-Authenticated-User-Email'].split(':')[-1]
    except:
        user = 'Dan Z'
    
    datetimestamp = str(datetime.datetime.now())
    
    if request.method == 'GET':
        #query = ''' SELECT title FROM `zproject201807.video_analysis1.video_metadata2` group by title '''
        query = ''' SELECT distinct title, gcs_url FROM `zproject201807.video_analysis1.video_metadata2` '''
        media_records = bq_query_table(query)
        
        # Enrich media records
        media_records_enriched = []
        for record in media_records:
            record_json = dict(record)
            media_records_enriched.append( record_json )
        
        return render_template('media.html', user=user, media_records_enriched=media_records_enriched)



############################################################
#
#   Media Detail
#
############################################################
@app.route('/media_detail/<record_title>', methods = ['GET','POST'])
#@flask_login.login_required
def media_detail(record_title):
    record_title = record_title
    
    try:
        user = request.headers['X-Goog-Authenticated-User-Email'].split(':')[-1]
    except:
        user = 'Dan Z'
    
    datetimestamp = str(datetime.datetime.now())
    
    if request.method == 'GET':
        bucket_name    = 'video-processing-dropzone-results'
        blob_name_json = '{}.json'.format(re.sub('\..+','',record_title))
        blob_name_text = '{}.txt'.format(re.sub('\..+','',record_title))
        
        result_json = json.loads(gcp_storage_download_as_string(bucket_name, blob_name_json))
        result_text = re.sub('\"$','',re.sub('^b\"','',str(gcp_storage_download_as_string(bucket_name, blob_name_text)))).strip()
        print('[ DEBUG ] json: {}'.format(type(result_json)))
        print('[ DEBUG ] text: {}'.format(type(result_text)))

        doc_sentiment, doc_sentiment_magnitude, sentiment_results = sample_analyze_sentiment(text_content=result_text)
        entity_results = sample_analyze_entity_sentiment(text_content=result_text)
        return render_template('media_detail.html', user=user, result_json=result_json, result_text=result_text, sentiment_results=sentiment_results)



############################################################
#
#   DataStudio Report
#
############################################################
@app.route('/report', methods = ['GET','POST'])
#@flask_login.login_required
def report():
    try:
        user = request.headers['X-Goog-Authenticated-User-Email'].split(':')[-1]
    except:
        user = 'Dan Z'
    
    datetimestamp = str(datetime.datetime.now())
    
    if request.method == 'GET':
        return render_template('report.html', user=user)



############################################################
#
#   Run App
#
############################################################
if __name__ == "__main__":
    #app.run(debug=True, threaded=True, host='0.0.0.0', port=5555)
    app.run(debug=True, threaded=True, host='0.0.0.0', port=8080)



#ZEND

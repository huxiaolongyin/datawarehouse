from flask import Flask, request, jsonify
import edge_tts
import base64
import asyncio

app = Flask(__name__)

@app.route("/text-to-speech", methods=['GET', 'POST'])
def text_to_speech():
    text = request.get_json()['text']
    voice= 'zh-CN-YunxiaNeural'

    audio_file = asyncio.run(text_to_speech_async(text, voice))
    
    response = {
        'message': 'Request received successfully',
        "voice": voice,
        'text': text,
        'status': 200,
        'audio_file': audio_file
    }
    return jsonify(response)

async def text_to_speech_async(text, voice) -> None:
    communicate = edge_tts.Communicate(text, voice)
    await communicate.save('./temp/output.mp3')
    with open("./temp/output.mp3", 'rb') as f:
        audio_file = base64.b64encode(f.read()).decode('utf-8')
    return audio_file

if __name__ == '__main__':
    app.run(debug=True)
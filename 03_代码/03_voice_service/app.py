from flask import Flask, request, jsonify
import edge_tts
import base64
import asyncio
import requests
app = Flask(__name__)

# 接口文件
@app.route("/text-to-speech", methods=['GET', 'POST'])
def text_to_speech():
    question_text = request.get_json()['question']
    voice= 'zh-CN-YunxiaNeural'
    ansewer = question(question_text)
    audio_file = asyncio.run(text_to_speech_async(ansewer, voice))

    # 回复接口内容
    response = {
        'message': 'Request received successfully',
        "voice": voice,
        'question_text': question_text,
        'ansewer': ansewer,
        'status': 200,
        'audio_file': audio_file
    }

    return jsonify(response)

# 提问，通过大模型，返回内容
def question(question_text) -> None:
	API_URL = 'https://ms-fc-aapp-func-bjjjauogaq.cn-shanghai.fcapp.run/invoke'
	payload = {"input":
				{"messages":[
					{"role":"system","content":"你是人工智能助手，名字叫笨笨同学"},
					{"role":"user", "content":"{}".format(question_text)}
					]},
				"parameters":
					{"do_sample":True,"max_length":512}}
	with requests.Session() as session:
		response = session.post(API_URL,json=payload)
	return response.json()['Data']['message']['content']

async def text_to_speech_async(text, voice) -> None:
    communicate = edge_tts.Communicate(text, voice)
    file_path = '03_代码/03_voice_service/temp/output.mp3'
    await communicate.save(file_path)
    with open(file_path, 'rb') as f:

        audio_file = base64.b64encode(f.read()).decode('utf-8')
    return audio_file

if __name__ == '__main__':
    app.run(debug=True)
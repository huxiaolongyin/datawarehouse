#coding=UTF-8
from flask import Flask, request, jsonify
import edge_tts
import base64
import asyncio
import requests
import ollama


app = Flask(__name__)

# 接口文件
@app.route("/text-to-speech", methods=['GET', 'POST'])
def text_to_speech():
    question_text = request.get_json()['question']
    voice= 'zh-CN-YunxiaNeural'
    ansewer_text = question(question_text)
    audio_file = asyncio.run(text_to_speech_async(ansewer_text, voice))

    # 回复接口内容
    response = {
        'message': 'Request received successfully',
        "voice": voice,
        'question_text': question_text,
        'ansewer': ansewer_text,
        'status': 200,
        'audio_file': audio_file
    }

    return jsonify(response)

# 提问，通过大模型，返回内容
def question(question_text) -> None:
    response = ollama.chat(model='llama3', messages=[
    {
        'role': 'system',
        'content': "你的名字是笨笨同学, 你是一个乐于助人、尊重和诚实的助手。请使用中文进行回答，请简要的用一句话回答问题，回答的字数不超过100字，请不要使用除了逗号和句号以外的任何字符，始终尽可能有帮助地回答。如果一个问题没有任何意义，或者与事实不符，请解释为什么，而不是回答不正确的问题。如果您不知道问题的答案，请不要分享虚假信息。"
    },
    {
        'role': 'user',
        'content': "{}".format(question_text),
    },
    
    ])
    return response['message']['content']

	# API_URL = 'https://ms-fc-aapp-func-bjjjauogaq.cn-shanghai.fcapp.run/invoke'
	# payload = {"input":
	# 			{"messages":[
	# 				{"role":"system","content":"你是人工智能助手，名字叫笨笨同学"},
	# 				{"role":"user", "content":"{}".format(question_text)}
	# 				]},
	# 			"parameters":
	# 				{"do_sample":True,"max_length":512}}
	# with requests.Session() as session:
	# 	response = session.post(API_URL,json=payload)
	# return response.json()['Data']['message']['content']

async def text_to_speech_async(text, voice) -> None:
    communicate = edge_tts.Communicate(text, voice)
    file_path = 'D:/code/DataWarehouse/03_代码/03_voice_service/temp/output.mp3'
    await communicate.save(file_path)
    with open(file_path, 'rb') as f:

        audio_file = base64.b64encode(f.read()).decode('utf-8')
    return audio_file

if __name__ == '__main__':
    app.run(debug=True)
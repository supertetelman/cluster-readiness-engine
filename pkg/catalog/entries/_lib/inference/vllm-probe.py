# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import time
import urllib.error
import urllib.request


def main():
    url = (os.environ.get('VLLM_URL') or ('http://' + os.environ['VLLM_SERVICE'] + ':8000')).rstrip('/')
    model = os.environ.get('VLLM_MODEL', 'Qwen/Qwen2.5-0.5B-Instruct')
    timeout = int(os.environ.get('TIMEOUT_SECONDS', '600'))
    if timeout <= 0:
        raise ValueError('TIMEOUT_SECONDS must be positive')
    deadline = time.monotonic() + timeout
    while True:
        try:
            with urllib.request.urlopen(url + '/health', timeout=min(10, timeout)) as response:
                if response.status == 200:
                    break
        except (OSError, urllib.error.URLError):
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError('vLLM did not become healthy before the deadline')
        time.sleep(min(2, max(0, deadline - time.monotonic())))
    request = urllib.request.Request(
        url + '/v1/chat/completions',
        data=json.dumps({'model': model, 'messages': [{'role': 'user', 'content': 'What is 2 + 2?'}],
                         'max_tokens': 32, 'temperature': 0}).encode(),
        headers={'Content-Type': 'application/json'}, method='POST')
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('inference probe deadline expired before the request')
    with urllib.request.urlopen(request, timeout=min(60, remaining)) as response:
        result = json.load(response)
    choices = result.get('choices', [])
    if not choices or not choices[0].get('message', {}).get('content', '').strip():
        raise ValueError('chat completion returned no assistant content')
    print(json.dumps({'model': model, 'status': 'passed'}), flush=True)


if __name__ == '__main__':
    main()

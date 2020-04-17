from __future__ import unicode_literals
import json
import os
import subprocess
import pprint
from queue import Queue
from bottle import route, run, Bottle, request, static_file, template
from threading import Thread
import youtube_dl
from pathlib import Path
from collections import ChainMap
from pathlib import Path
import unicodedata
import string

app = Bottle()

token = os.environ.get('YDL_TOKEN', 'youtube-dl')
outdir = os.environ.get('YDL_OUTDIR', '/youtube-dl')
if not outdir == '':
    outdir = '.'
if not outdir.endswith('/'):
    outdir += "/"


app_defaults = {
    'YDL_FORMAT': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]',
    'YDL_EXTRACT_AUDIO_FORMAT': None,
    'YDL_EXTRACT_AUDIO_QUALITY': '192',
    'YDL_RECODE_VIDEO_FORMAT': None,
    'YDL_OUTPUT_TEMPLATE': '{title} [{id}].{ext}',
    'YDL_ARCHIVE_FILE': None,
    'YDL_SERVER_HOST': '0.0.0.0',
    'YDL_SERVER_PORT': 8080,
}


class SanitizedFilenameTmpl:
    """Format string that evaluates to a safe filename."""

    whitelist = "-_.() %s%s'\"" % (
        string.ascii_letters,
        string.digits,
    )  #: string containing all allowed filename characters
    char_limit = 128  #: max length of allowed filenames
    # Technically, the limit is 255 - len('.part'), but 128 is much more
    # reasonable for human consumption

    def __init__(self, tmpl):
        self._tmpl = tmpl

    def _sanitize(self, val):
        # 1. Replace unicode with ascii-equivalents
        val = (
            unicodedata.normalize('NFKD', val)
            .encode('ASCII', 'ignore')
            .decode()
        )
        # 2. Throw away any non-whitelisted characters
        val = ''.join(c for c in val if c in self.whitelist)
        return val.strip()

    def format(self, **kwargs):
        keys = set(
            [
                t[1]
                for t in string.Formatter().parse(self._tmpl)
                if t[1] is not None
            ]
        )
        sanitized_kwargs = {
            key: self._sanitize(kwargs.get(key, '')) for key in keys
        }
        filename = self._tmpl.format(**sanitized_kwargs)

        while "  " in filename:  # eliminate double spaces
            filename = filename.replace("  ", " ")

        while len(filename) > self.char_limit:
            # if filename is too long, elimitate characters from the longest
            # field
            len_overflow = len(filename) - self.char_limit
            longest_val = max(sanitized_kwargs.values(), key=len)
            shortened_val = longest_val[:-len_overflow].strip()
            sanitized_kwargs = {
                key: shortened_val if val == longest_val else val
                for (key, val) in sanitized_kwargs.items()
            }
            filename = self._tmpl.format(**sanitized_kwargs)
        return filename.strip()


@app.route('/' + token)
def dl_queue_list():
    index_html = (Path(__file__).parent / 'index.html').read_text()
    return template(index_html, token=token)


@app.route('/' + token + '/static/:filename#.*#')
def server_static(filename):
    return static_file(filename, root='./static')


@app.route('/' + token + '/q', method='GET')
def q_size():
    return {"success": True, "size": json.dumps(list(dl_q.queue))}


@app.route('/' + token + '/q', method='POST')
def q_put():
    url = request.forms.get("url")
    options = {'format': request.forms.get("format")}

    if not url:
        return {
            "success": False,
            "error": "/q called without a 'url' query param",
        }

    ydl_options = get_ydl_options(options)
    with youtube_dl.YoutubeDL(ydl_options) as ydl:
        info = ydl.extract_info(url, download=False)
        pprint.pprint(info)
        outfile = SanitizedFilenameTmpl(ydl_options['outtmpl']).format(**info)
        ydl.params['outtmpl'] = str(Path(outdir) / outfile)
        dl_q.put((ydl, url))
        print("Added url " + url + " to the download queue")
        return {
            "success": True,
            "url": url,
            "options": options,
            "outfile": outfile,
        }


@app.route("/" + token + "/update", method="GET")
def update():
    command = ["pip", "install", "--upgrade", "youtube-dl"]
    proc = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    output, error = proc.communicate()
    return {"output": output.decode('ascii'), "error": error.decode('ascii')}


def dl_worker():
    while not done:
        ydl, url = dl_q.get()
        ydl.download([url])
        dl_q.task_done()


def get_ydl_options(request_options):
    request_vars = {
        'YDL_EXTRACT_AUDIO_FORMAT': None,
        'YDL_RECODE_VIDEO_FORMAT': None,
    }

    requested_format = request_options.get('format', 'bestvideo')

    if requested_format in [
        'aac',
        'flac',
        'mp3',
        'm4a',
        'opus',
        'vorbis',
        'wav',
    ]:
        request_vars['YDL_EXTRACT_AUDIO_FORMAT'] = requested_format
    elif requested_format == 'bestaudio':
        request_vars['YDL_EXTRACT_AUDIO_FORMAT'] = 'best'
    elif requested_format in ['mp4', 'flv', 'webm', 'ogg', 'mkv', 'avi']:
        request_vars['YDL_RECODE_VIDEO_FORMAT'] = requested_format

    ydl_vars = ChainMap(request_vars, os.environ, app_defaults)

    postprocessors = []

    if ydl_vars['YDL_EXTRACT_AUDIO_FORMAT']:
        postprocessors.append(
            {
                'key': 'FFmpegExtractAudio',
                'preferredcodec': ydl_vars['YDL_EXTRACT_AUDIO_FORMAT'],
                'preferredquality': ydl_vars['YDL_EXTRACT_AUDIO_QUALITY'],
            }
        )

    if ydl_vars['YDL_RECODE_VIDEO_FORMAT']:
        postprocessors.append(
            {
                'key': 'FFmpegVideoConvertor',
                'preferedformat': ydl_vars['YDL_RECODE_VIDEO_FORMAT'],
            }
        )

    return {
        'format': ydl_vars['YDL_FORMAT'],
        'postprocessors': postprocessors,
        'outtmpl': ydl_vars['YDL_OUTPUT_TEMPLATE'],
        'download_archive': ydl_vars['YDL_ARCHIVE_FILE'],
    }


dl_q = Queue()
done = False
dl_thread = Thread(target=dl_worker)
dl_thread.start()

print("Updating youtube-dl to the newest version")
updateResult = update()
print(updateResult["output"])
print(updateResult["error"])

print("Started download thread")

app_vars = ChainMap(os.environ, app_defaults)

app.run(
    host=app_vars['YDL_SERVER_HOST'],
    port=app_vars['YDL_SERVER_PORT'],
    debug=True,
)
done = True
print("Shutting down")
dl_thread.join()

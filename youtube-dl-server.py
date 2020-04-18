"""Web app wrapping around youtube-dl."""
import json
import os
import pprint
import string
import subprocess
import unicodedata
from pathlib import Path
from queue import Queue
from threading import Thread

import bottle
import youtube_dl

TOKEN = os.environ.get('YDL_TOKEN', 'youtube-dl')
OUTDIR = os.environ.get('YDL_OUTDIR', '')
if OUTDIR == '':
    OUTDIR = '.'
if not OUTDIR.endswith('/'):
    OUTDIR += "/"

YDL_CHOWN_UID = os.environ.get('YDL_CHOWN_UID', None)
YDL_CHOWN_GID = os.environ.get('YDL_CHOWN_GID', -1)
YDL_OUTPUT_TEMPLATE = '{title} [{id}]'
YDL_ARCHIVE_FILE = os.environ.get('YDL_ARCHIVE_FILE', None)
YDL_SERVER_HOST = os.environ.get('YDL_SERVER_HOST', '0.0.0.0')
YDL_SERVER_PORT = int(os.environ.get('YDL_SERVER_PORT', 8080))

FORMATS = {
    'smallmp4': 'mp4[height<=480]/best[ext=mp4]',
    'normalmp4': 'mp4[height<=720]/best[ext=mp4]',
    'bestmp4': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]',
    'mp3': 'bestaudio/best',
}

EXTENSIONS = {
    'smallmp4': 'mp4',
    'normalmp4': 'mp4',
    'bestmp4': 'mp4',
    'mp3': 'mp3',
}

DL_Q = Queue()

APP = bottle.Bottle()


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


@APP.route('/' + TOKEN)
def dl_queue_list():
    index_html = (Path(__file__).parent / 'index.html').read_text()
    return bottle.template(index_html, token=TOKEN)


@APP.route('/' + TOKEN + '/static/:filename#.*#')
def server_static(filename):
    return bottle.static_file(filename, root='./static')


@APP.route('/' + TOKEN + '/result/:filename#.*#')
def result_file(filename):
    if (Path(OUTDIR) / filename).is_file():
        return bottle.static_file(filename, root=OUTDIR)
    else:
        result_html = (
            Path(__file__).parent / 'result_not_available.html'
        ).read_text()
        return bottle.template(result_html, token=TOKEN, outfile=filename)


@APP.route('/' + TOKEN + '/q', method='GET')
def q_size():
    return {"success": True, "size": json.dumps(list(DL_Q.queue))}


@APP.route('/' + TOKEN + '/q', method='POST')
def q_put():
    url = bottle.request.forms.get("url")
    options = {'format': bottle.request.forms.get("format")}
    return_json = bottle.request.forms.get("return_json", 'true')

    if not url:
        return {
            "success": False,
            "error": "/q called without a 'url' query param",
        }

    ydl_options = get_ydl_options(options)
    print("ydl_options = %s" % pprint.pformat(ydl_options))
    with youtube_dl.YoutubeDL(ydl_options) as ydl:
        info = ydl.extract_info(url, download=False)
        # print("info = %s" % pprint.pformat(info))
        ext = ydl_options['extension']
        outfile = (
            SanitizedFilenameTmpl(YDL_OUTPUT_TEMPLATE).format(**info)
            + "."
            + ext
        )
        ydl.params['outtmpl'] = str(Path(OUTDIR) / outfile)
        DL_Q.put((ydl, url))
        print("Added url " + url + " to the download queue")
        if return_json == 'true':
            return {
                "success": True,
                "url": url,
                "options": options,
                "outfile": outfile,
            }
        else:
            result_html = (Path(__file__).parent / 'result.html').read_text()
            return bottle.template(
                result_html, token=TOKEN, url=url, outfile=outfile
            )


@APP.route("/" + TOKEN + "/update", method="GET")
def update():
    command = ["pip", "install", "--upgrade", "youtube-dl"]
    proc = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    output, error = proc.communicate()
    return {"output": output.decode('ascii'), "error": error.decode('ascii')}


def get_ydl_options(request_options):
    """Generate options for YoutubeDL from http request options."""
    requested_format = request_options.get('format', 'normalmp4')
    ext = EXTENSIONS.get(requested_format, 'mp4')

    default_format = FORMATS['normalmp4']
    fmt = FORMATS.get(requested_format, default_format)

    postprocessors = []
    if fmt == 'mp3':
        postprocessors.append(
            {
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }
        )

    return {
        'format': fmt,
        'extension': ext,  # This is actually not part of YoutubeDL
        'noplaylist': True,
        'postprocessors': postprocessors,
        'outtmpl': YDL_OUTPUT_TEMPLATE,
        'download_archive': YDL_ARCHIVE_FILE,
    }


def dl_worker():
    """Process downloads from the DL_Q.

    This is the main function of the download thread.
    """
    while True:
        try:
            ydl, url = DL_Q.get()
            if ydl is None:
                return  # end the download thread with poison pill
            outfile = ydl.params['outtmpl']
            if Path(outfile).is_file():
                print("Removing existing %r" % outfile)
                Path(outfile).unlink()
            ydl.download([url])
            if YDL_CHOWN_UID is not None:
                os.chown(
                    outfile, uid=int(YDL_CHOWN_UID), gid=int(YDL_CHOWN_GID)
                )
            print("Downloaded to %r" % outfile)
            DL_Q.task_done()
        except Exception as exc_info:
            print("Exception: %r" % (exc_info,))


def main():
    """Run APP.

    This is the main function for the main thread.
    """
    dl_thread = Thread(target=dl_worker)
    dl_thread.start()

    print("Updating youtube-dl to the newest version")
    updateResult = update()
    print(updateResult["output"])
    print(updateResult["error"])

    print("Started download thread")

    APP.run(
        host=YDL_SERVER_HOST, port=YDL_SERVER_PORT, debug=True,
    )
    print("Shutting down")
    DL_Q.put((None, None))  # poison pill for download thread
    dl_thread.join()


if __name__ == "__main__":
    main()

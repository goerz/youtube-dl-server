"""Web app wrapping around youtube-dl."""
import json
import logging
import os
import pprint
import string
import subprocess
import textwrap
import unicodedata
from collections import defaultdict
from functools import partial, wraps
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
YDL_LOGFILE = os.environ.get('YDL_LOGFILE', 'youtube-dl-server.log')
YDL_DL_LOGFILE = os.environ.get('YDL_LOGFILE', 'youtube-dl.log')
try:
    _YDL_LOGLEVEL = os.environ.get('YDL_LOGLEVEL', 'INFO').upper()
    YDL_LOGLEVEL = getattr(logging, _YDL_LOGLEVEL)
except AttributeError:
    print("WARNING: invalid YDL_LOGLEVEL %r. Using INFO" % _YDL_LOGLEVEL)
    YDL_LOGLEVEL = logging.INFO

YDL_DEFAULT_PROFILE = os.environ.get('YDL_DEFAULT_PROFILE', 'normalmp4')

FORMATS = {  # profile => YoutubeDL format
    'smallmp4': 'mp4[height<=480]/best[ext=mp4]',
    'normalmp4': 'mp4[height<=720]/best[ext=mp4]',
    'bestmp4': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]',
    'mp3': 'bestaudio/best',
}

POSTPROCESSORS = defaultdict(list)
# profile => postprocessor settings
POSTPROCESSORS['mp3'] = [
    {
        'key': 'FFmpegExtractAudio',
        'preferredcodec': 'mp3',
        'preferredquality': '192',
    }
]

EXTENSIONS = {  # profile => file extension
    'smallmp4': 'mp4',
    'normalmp4': 'mp4',
    'bestmp4': 'mp4',
    'mp3': 'mp3',
}

DL_Q = Queue()

MAIN_LOGGER = logging.getLogger('youtubedl-server')


def configure_logging(
    logger, logfile=None, log_to_stdout=True, level=logging.INFO
):
    """Set up the given `logger`."""
    logger.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s %(name)s [%(levelname)s]  %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    msg = "Set to log at level %r to " % logging.getLevelName(level)
    if logfile is not None:
        file_handler = logging.FileHandler(logfile)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        msg += str(logfile)
        if log_to_stdout:
            msg += " and "
    if log_to_stdout:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        msg += "stdout."
    logger.debug(msg)
    return logger


def log_to_logger(fn, logger):
    """Bottle pluging that wraps request for logging."""

    @wraps(fn)
    def _log_to_logger(*args, **kwargs):
        actual_response = fn(*args, **kwargs)
        # modify this to log exactly what you need:
        logger.info(
            'Request from %s %s %s %s',
            bottle.request.remote_addr,
            bottle.request.method,
            bottle.request.url,
            bottle.response.status,
        )
        return actual_response

    return _log_to_logger


configure_logging(
    MAIN_LOGGER, logfile=YDL_LOGFILE, log_to_stdout=True, level=YDL_LOGLEVEL
)
APP = bottle.Bottle()
APP.install(partial(log_to_logger, logger=MAIN_LOGGER))


@APP.route('/' + TOKEN)
def dl_form():
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
    return_json = bottle.request.forms.get("return_json", 'true')
    if not url:
        return {
            "success": False,
            "error": "/q called without a 'url' query param",
        }
    # TODO: rename 'format' in request to 'profile'
    profile = bottle.request.forms.get("format", YDL_DEFAULT_PROFILE)

    result = submit_download(url, profile)

    if return_json == 'true':
        return result
    else:
        # TODO: give error message
        result_html = (Path(__file__).parent / 'result.html').read_text()
        outfile = result['outfile']
        return bottle.template(
            result_html, token=TOKEN, url=url, outfile=outfile
        )


def youtube_dl_show_progress(d, logger):
    """Log download progress in :class:`YoutubeDL` instance.

    After `logger` is set via :func:`functools.partial`, the resulting function
    is used as a "progress_hook" for :class:`YoutubeDL`.
    """
    if d['status'] == 'error':
        logger.error("Failed to download %r", d['filename'])
    elif d['status'] == 'finished':
        logger.info("Finished downloading %r", d['filename'])
    elif d['status'] == 'downloading':
        try:
            total_bytes = d['total_bytes']
        except (ValueError, TypeError):
            total_bytes = d.get('total_estimates_bytes', None)
        downloaded_MB = "%.1f" % (float(d['downloaded_bytes']) / 1048576.0)
        try:
            total_MB = "%.1f" % (float(total_bytes) / 1048576.0)
            percent = "%d" % (
                100
                * float(d['downloaded_bytes'])
                / float(total_bytes)
            )
        except TypeError:
            percent = '???'
        try:
            speed = "%.2f" % (float(d['speed']) / 1048576.0)
        except TypeError:
            speed = '???'
        logger.info(
            "Downloaded %s/%s MB (%s%%, %s MB/s)",
            downloaded_MB,
            total_MB,
            percent,
            speed,
        )


def submit_download(url, profile):
    """Send `url` to download-thread for downloading with given `profile`.

    Returns a dict with the following values:

    * `success`: whether youtube-dl could successfully identify a video at the
       given `url`
    * `url`: the input `url`
    * `profile`: the input `profile`
    * `format`: the youtube-dl format code associated with the `profile`
    * `outfile`: the name of the output file (inside YDL_OUTDIR)

    If `success`, a tuple consisting of a YoutubeDL instance (initialized for
    the given `profile`) and the url will be placed on the DL_Q Queue, to be
    processed by the download-thread.
    """
    fmt = FORMATS.get(profile, FORMATS[YDL_DEFAULT_PROFILE])
    ydl_params = {
        'format': fmt,
        'noplaylist': True,
        'postprocessors': POSTPROCESSORS[profile],
        'outtmpl': YDL_OUTPUT_TEMPLATE,
        'download_archive': YDL_ARCHIVE_FILE,
        'progress_hooks': [],
        'quiet': True,
        'no_warnings': True,
    }
    if YDL_LOGLEVEL == logging.DEBUG:
        MAIN_LOGGER.debug(
            textwrap.indent(
                "\nydl_params = %s" % pprint.pformat(ydl_params), '    '
            )
        )
    with youtube_dl.YoutubeDL(ydl_params) as ydl:
        try:
            info = ydl.extract_info(url, download=False)
        except Exception as exc_info:
            MAIN_LOGGER.error("Exception: %r", exc_info)
            success = False
            outfile = None
            MAIN_LOGGER.error(
                "Could not add url %r to the download queue", url
            )
        else:
            success = True
            if YDL_LOGLEVEL == logging.DEBUG:
                MAIN_LOGGER.debug(
                    textwrap.indent(
                        "\ninfo = %s" % pprint.pformat(info), '    '
                    )
                )
            ext = EXTENSIONS.get(profile, 'mp4')
            outfile = (
                SanitizedFilenameTmpl(YDL_OUTPUT_TEMPLATE).format(**info)
                + "."
                + ext
            )
            outpath = str(Path(OUTDIR) / outfile)
            logfile = Path(outpath).with_suffix('.log')
            dl_logger = logging.getLogger('youtubedl.%s' % info['id'])
            configure_logging(
                dl_logger,
                logfile=logfile,
                log_to_stdout=True,
                level=logging.INFO,
            )
            ydl.params['outtmpl'] = outpath
            ydl.params['logger'] = dl_logger
            ydl.add_progress_hook(
                partial(youtube_dl_show_progress, logger=dl_logger)
            )
            DL_Q.put((ydl, url))
            MAIN_LOGGER.info("Added url %r to the download queue", url)

        return {
            "success": success,
            "url": url,
            "profile": profile,
            "format": fmt,
            "outfile": outfile,
        }


@APP.route("/" + TOKEN + "/update", method="GET")
def update():
    command = ["pip", "install", "--upgrade", "youtube-dl"]
    proc = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    output, error = proc.communicate()
    return {"output": output.decode('ascii'), "error": error.decode('ascii')}


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


def dl_worker():
    """Process downloads from the DL_Q.

    This is the main function of the download thread.
    """
    logger = logging.getLogger('youtubedl')
    configure_logging(
        logger, logfile=YDL_DL_LOGFILE, log_to_stdout=True, level=YDL_LOGLEVEL
    )
    while True:
        try:
            ydl, url = DL_Q.get()
            if ydl is None:
                return  # end the download thread with poison pill
            outfile = ydl.params['outtmpl']
            if Path(outfile).is_file():
                logger.info("Removing existing %r", outfile)
                Path(outfile).unlink()
            ydl.download([url])
            if YDL_CHOWN_UID is not None:
                os.chown(
                    outfile, uid=int(YDL_CHOWN_UID), gid=int(YDL_CHOWN_GID)
                )
            logger.info("Downloaded to %r", outfile)
            DL_Q.task_done()
        except Exception as exc_info:
            logger.error("Exception: %r", exc_info)


def main():
    """Run APP.

    This is the main function for the main thread.
    """
    dl_thread = Thread(target=dl_worker)
    dl_thread.start()
    MAIN_LOGGER.info("Started download thread")

    MAIN_LOGGER.info("Updating youtube-dl to the newest version")
    updateResult = update()
    MAIN_LOGGER.info(updateResult["output"].strip())
    MAIN_LOGGER.info(updateResult["error"].strip())
    APP.run(
        host=YDL_SERVER_HOST, port=YDL_SERVER_PORT, quiet=True,
    )
    MAIN_LOGGER.info("Shutting down")
    DL_Q.put((None, None))  # poison pill for download thread
    dl_thread.join()


if __name__ == "__main__":
    main()

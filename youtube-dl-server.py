"""Web app wrapping around youtube-dl."""
import json
import logging
import os
import pprint
import string
import subprocess
import textwrap
import time
import unicodedata
from collections import defaultdict
from functools import partial, wraps
from pathlib import Path
from queue import Queue
from threading import Thread
from urllib.parse import urlencode
from urllib.request import pathname2url

import bottle
import youtube_dl


YDL_USERS = os.environ.get('YDL_USERS', 'youtube-dl:testing:./')


def process_users(ydl_users):
    """Process YDL_USERS specification."""
    tokens = {}
    outdirs = {}
    uids = {}
    gids = {}
    for spec in ydl_users.split(";"):
        username, token, outdir, uid, gid = (spec + "::::").split(":")[:5]
        tokens[username] = token
        outdirs[username] = outdir or './'
        if not outdirs[username].endswith("/"):
            outdirs[username] += "/"
        uids[username] = uid or None
        gids[username] = gid or None
    return tokens, outdirs, uids, gids


TOKENS, OUTDIRS, UIDS, GIDS = process_users(YDL_USERS)

YDL_OUTPUT_TEMPLATE = '{title} [{id}]'
YDL_SERVER_HOST = os.environ.get('YDL_SERVER_HOST', '0.0.0.0')
YDL_SERVER_PORT = int(os.environ.get('YDL_SERVER_PORT', 8080))
YDL_LOGFILE = os.environ.get('YDL_LOGFILE', 'youtube-dl-server.log')
YDL_DL_LOGFILE = os.environ.get('YDL_DL_LOGFILE', 'youtube-dl.log')
try:
    _YDL_LOGLEVEL = os.environ.get('YDL_LOGLEVEL', 'INFO').upper()
    YDL_LOGLEVEL = getattr(logging, _YDL_LOGLEVEL)
except AttributeError:
    print("WARNING: invalid YDL_LOGLEVEL %r. Using INFO" % _YDL_LOGLEVEL)
    YDL_LOGLEVEL = logging.INFO

YDL_DEFAULT_PRESET = os.environ.get('YDL_DEFAULT_PRESET', 'normalmp4')

FORMATS = {  # preset => YoutubeDL format
    'smallmp4': 'mp4[height<=480]/best[ext=mp4]',
    'normalmp4': 'mp4[height<=720]/best[ext=mp4]',
    'bestmp4': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]',
    'mp3': 'bestaudio/best',
}

POSTPROCESSORS = defaultdict(list)
# preset => postprocessor settings
POSTPROCESSORS['mp3'] = [
    {
        'key': 'FFmpegExtractAudio',
        'preferredcodec': 'mp3',
        'preferredquality': '192',
    }
]

EXTENSIONS = {  # preset => file extension
    'smallmp4': 'mp4',
    'normalmp4': 'mp4',
    'bestmp4': 'mp4',
    'mp3': 'mp3',
}

MIMETYPES = {
    '.mp4': 'video/mp4',
    '.mp3': 'audio/mpeg',
    '.log': 'text/plain',
}

DL_Q = Queue()

MAIN_LOGGER = logging.getLogger('youtubedl-server')

TEMPLATES = Path(__file__).parent / 'templates'


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


def is_authorized(username, token):
    """Check whether the username/token combination is valid."""
    if username not in TOKENS or TOKENS[username] != token:
        time.sleep(1)  # make brute-forcing tokens very difficult
        return False
    return True


@APP.route('/<username>')
def dl_form(username):
    """Route for the root page (form for submitting a video)."""
    token = bottle.request.params.get("token", None)
    if not is_authorized(username, token):
        bottle.abort(401, "Not authorized")
    template = (TEMPLATES / 'page.j2').read_text()
    content = bottle.template(
        (TEMPLATES / 'form.j2').read_text(), username=username, token=token
    )
    return bottle.template(template, title="youtube-dl", content=content)


@APP.route('/<username>/list')
def list_files(username):
    """Route for listing downloaded files."""
    token = bottle.request.params.get("token", None)
    if not is_authorized(username, token):
        bottle.abort(401, "Not authorized")
    template = (TEMPLATES / 'page.j2').read_text()
    content = [
        '<h1 class="display-4">',
        '<a href="/{username}?token={token}">youtube-dl</a>'.format(
            username=username, token=token
        ),
        '</h1>' '<div class="text-left">',
        '<p>Available files:</p>',
        '<ul>',
    ]
    for filename in Path(OUTDIRS[username]).iterdir():
        if filename.is_file() and filename.suffix in ('.mp4', '.mp3'):
            content.append(
                '<li><a href="/%s/result/%s">%s</a></li>'
                % (username, pathname2url(str(filename.name)), filename.name)
            )
    content.append('</ul></div>')
    return bottle.template(
        template, title="youtube-dl", content="\n".join(content)
    )


@APP.route('/static/:filename#.*#')
def server_static(filename):
    """Route static resources, e.g. CSS files."""
    return bottle.static_file(filename, root='./static')


@APP.route('/<username>/submit', method='POST')
def submit(username):
    """Route for submitting a request to download a video."""
    return_json = bottle.request.params.get("return_json", 'true')
    token = bottle.request.params.get("token", None)
    if not is_authorized(username, token):
        if return_json == 'true':
            return {
                "success": False,
                "error": "not authorized",
            }
        else:
            bottle.abort(401, "Not authorized")
    url = bottle.request.params.get("url")
    if not url:
        if return_json == 'true':
            return {
                "success": False,
                "error": "missing 'url' query param",
            }
        else:
            bottle.abort(400, "missing 'url' query param")
    preset = bottle.request.params.get("preset", YDL_DEFAULT_PRESET)
    result = submit_download(username, url, preset)

    if return_json == 'true':
        return result
    else:
        if result['success']:
            outfile = result['outfile']
            # Redirect to resulting file (https://httpstatuses.com/303)
            bottle.redirect(
                "/%s/result/%s?%s"
                % (
                    username,
                    pathname2url(outfile),
                    urlencode({'url': url, 'download': 'false'}),
                ),
                code=303,
            )
        else:
            return result


@APP.route('/<username>/result/:filename#.*#')
def result_file(username, filename):
    """Route for obtaining a downloaded file.

    Depending on a `download` parameter in the HTTP request ('true'/'false'),
    either try to download the file directly, or render an HTML page containing
    a direct link to the file. The latter may be preferable because then a use
    can right-click on the link and choose what do do with it.

    No authentication token is used. This is so that a "result" link can be
    shared publicly without exposing the token.
    """
    try:
        filename = Path(filename).name  # protect against escaping from OUTDIR
        logfile = (Path(OUTDIRS[username]) / filename).with_suffix('.log')
        download = bottle.request.params.get("download", 'true')
        exists = (Path(OUTDIRS[username]) / filename).is_file()
    except (OSError, KeyError):
        # protect against e.g. too long filenames
        bottle.abort(404, "Invalid filename")

    if download == 'true':
        if exists:
            return bottle.static_file(
                filename,
                root=OUTDIRS[username],
                mimetype=MIMETYPES.get(Path(filename).suffix, True),
            )
        else:
            bottle.abort(404, "No file %s" % filename)
    content = '<h1 class="display-4">youtube-dl</h1>\n'
    url = bottle.request.params.get("url", None)
    if exists:
        if url:
            content += r'''
            <p>The video at <code><a href="{url}">{url}</a></code> has been processed.</p>
            <p>Download the resulting file:</p>
            <p class="lead"><a href="/{username}/result/{filename_enc}">{filename}</a></p>
            '''.format(
                username=username,
                url=url,
                filename_enc=pathname2url(filename),
                filename=filename,
            )
        else:
            content += r'''
            <p>Download the completed file:</p>
            <p class="lead"><a href="/{username}/result/{filename_enc}">{filename}</a></p>
            '''.format(
                username=username,
                filename_enc=pathname2url(filename),
                filename=filename,
            )
    else:  # file does not (yet) exist
        if logfile.is_file():
            loglink = (
                ' (see <a href="/%s/result/%s?download=true">log file</a>)'
                % (username, logfile.name)
            )
        else:
            loglink = ''
        if url:
            content += r'''
            <p>The video at <code><a href="{url}">{url}</a></code> is still being processed{loglink}.</p>
            <p>Wait for youtube-dl to complete, then download the resulting file:</p>
            <p class="lead"><a href="/{username}/result/{filename_enc}?{params}">{filename}</a></p>
            '''.format(
                username=username,
                url=url,
                loglink=loglink,
                filename_enc=pathname2url(filename),
                filename=filename,
                params=urlencode({'download': 'false', 'url': url}),
            )
        else:
            content += r'''
            <p>The requested file may still be processing{loglink}.</p>
            <p>Wait for youtube-dl to complete, then download the resulting file:</p>
            <p class="lead"><a href="/{username}/result/{filename_enc}?download=false">{filename}</a></p>
            '''.format(
                username=username,
                filename=filename,
                filename_enc=pathname2url(filename),
                loglink=loglink,
            )
    template = (TEMPLATES / 'page.j2').read_text()
    return bottle.template(template, title=filename, content=content)


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
                100 * float(d['downloaded_bytes']) / float(total_bytes)
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


def submit_download(username, url, preset):
    """Send `url` to download-thread for downloading with given `preset`.

    Returns a dict with the following values:

    * `success`: whether youtube-dl could successfully identify a video at the
       given `url`
    * `url`: the input `url`
    * `preset`: the input `preset`
    * `format`: the youtube-dl format code associated with the `preset`
    * `outfile`: the name of the output file (inside YDL_OUTDIR)

    If `success`, a tuple consisting of a YoutubeDL instance (initialized for
    the given `preset`), the username, and the url will be placed on the DL_Q
    Queue, to be processed by the download-thread.
    """
    fmt = FORMATS.get(preset, FORMATS[YDL_DEFAULT_PRESET])
    ydl_params = {
        'format': fmt,
        'noplaylist': True,
        'postprocessors': POSTPROCESSORS[preset],
        'outtmpl': YDL_OUTPUT_TEMPLATE,
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
            ext = EXTENSIONS.get(preset, 'mp4')
            outfile = (
                SanitizedFilenameTmpl(YDL_OUTPUT_TEMPLATE).format(**info)
                + "."
                + ext
            )
            outpath = str(Path(OUTDIRS[username]) / outfile)
            logfile = Path(outpath).with_suffix('.log')
            dl_logger = logging.getLogger('youtubedl.%s' % info['id'])
            configure_logging(
                dl_logger,
                logfile=logfile,
                log_to_stdout=True,
                level=YDL_LOGLEVEL,
            )
            ydl.params['outtmpl'] = outpath
            ydl.params['logger'] = dl_logger
            ydl.add_progress_hook(
                partial(youtube_dl_show_progress, logger=dl_logger)
            )
            DL_Q.put((ydl, username, url))
            MAIN_LOGGER.info("Added url %r to the download queue", url)

        return {
            "success": success,
            "url": url,
            "preset": preset,
            "format": fmt,
            "outfile": outfile,
        }


@APP.route("/update", method="GET")
def update():
    """Update the youtube-dl backend."""
    token = bottle.request.params.get("token", None)
    if token not in TOKENS.values():
        bottle.abort(401, "Not authorized")
    return _update()


def _update():
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
            ydl, username, url = DL_Q.get()
            if ydl is None:
                return  # end the download thread with poison pill
            outfile = ydl.params['outtmpl']
            if Path(outfile).is_file():
                logger.info("Removing existing %r", outfile)
                Path(outfile).unlink()
            ydl.download([url])
            if UIDS.get(username, None) is not None:
                uid = int(UIDS[username])
                gid = int(GIDS.get(username, uid))
                os.chown(outfile, uid=uid, gid=gid)
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
    updateResult = _update()
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

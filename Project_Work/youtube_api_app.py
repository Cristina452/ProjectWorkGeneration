from flask import Flask, jsonify
from youtube_scraper import YoutubeScraper

scraper = YoutubeScraper()
app = Flask(__name__)


@app.route('/youtubeapi/v1/videos/<int:category>', methods=['Get'])
def videos_by_cat(category):
    vids = scraper.get_videos_by_category(50, "US", category)
    return jsonify(vids)


@app.route('/youtubeapi/v1/channels/search/<string:query>', methods=["Get"])
def channels_search(query):
    chans, query = scraper.get_channels(50, "US", str(query))
    return jsonify(chans)


@app.route('/youtubeapi/v1/videos/<string:channelid>)',methods=["Get"])
def videos_by_chann(channelid):
    vids = scraper.get_video_by_channel(channelid)
    return jsonify(vids)


if __name__ == '__main__':
    app.run(debug=True)

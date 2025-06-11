import json
import random
import time
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException

class SpotifyConnection:
    def __init__(self, clientId, clientSecret, redirectUri, scope, cachePath=".cache"):
        self.authManager = SpotifyOAuth(
            client_id=clientId,
            client_secret=clientSecret,
            redirect_uri=redirectUri,
            scope=scope,
            cache_path=cachePath
        )
        self.client = Spotify(auth_manager=self.authManager)
        self.userId = self.client.current_user()["id"]

    def _withRetry(self, func, *args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except SpotifyException as e:
                if e.http_status == 429:
                    retryAfter = int(e.headers.get("Retry-After", 1))
                    print(f"Rate limited. Sleeping {retryAfter}s")
                    time.sleep(retryAfter)
                else:
                    raise

    def getPlaylistIdByName(self, playlistName):
        results = self._withRetry(self.client.current_user_playlists, limit=50)
        while results:
            for p in results["items"]:
                if p["name"] == playlistName:
                    return p["id"]
            if results.get("next"):
                results = self._withRetry(self.client.next, results)
            else:
                break
        return None

    def getOrCreatePlaylist(self, playlistName, description=""):
        playlistId = self.getPlaylistIdByName(playlistName)
        if not playlistId:
            newPl = self._withRetry(
                self.client.user_playlist_create,
                user=self.userId,
                name=playlistName,
                public=False,
                description=description
            )
            playlistId = newPl["id"]
        return playlistId

    def getPlaylistTracks(self, playlistId):
        trackIds = []
        results = self._withRetry(
            self.client.playlist_items,
            playlist_id=playlistId,
            limit=100,
            fields="items.track.id,next",
            additional_types=["track"]
        )
        while results:
            for item in results["items"]:
                track = item.get("track")
                if track and track.get("id"):
                    trackIds.append(track["id"])
            if results.get("next"):
                results = self._withRetry(self.client.next, results)
            else:
                break
        return trackIds

    def getLikedTracks(self):
        trackIds = []
        offset = 0
        while True:
            results = self._withRetry(self.client.current_user_saved_tracks, limit=50, offset=offset)
            items = results.get("items", [])
            if not items:
                break
            for item in items:
                track = item.get("track")
                if track and track.get("id"):
                    trackIds.append(track["id"])
            offset += len(items)
        return trackIds

    def getUserTopTracks(self, maxTracks=200, timeRange="medium_term"):
        allIds = []
        for offset in range(0, maxTracks, 50):
            limitPerRequest = min(50, maxTracks - offset)
            results = self._withRetry(
                self.client.current_user_top_tracks,
                limit=limitPerRequest,
                offset=offset,
                time_range=timeRange
            )
            items = results.get("items", [])
            if not items:
                break
            allIds.extend([t["id"] for t in items])
            if len(items) < limitPerRequest:
                break
        return allIds

    def getTracksInfo(self, trackIds):
        info = {}
        for i in range(0, len(trackIds), 50):
            batch = trackIds[i:i+50]
            results = self._withRetry(self.client.tracks, batch)
            for t in results["tracks"]:
                if t:
                    name = t["name"]
                    artists = t.get("artists", [])
                    if artists:
                        artistName = artists[0]["name"]
                        artistId = artists[0]["id"]
                    else:
                        artistName = "Unknown"
                        artistId = None
                    info[t["id"]] = (name, artistName, artistId)
        return info

    def clearPlaylist(self, playlistId):
        self._withRetry(self.client.playlist_replace_items, playlist_id=playlistId, items=[])

    def addTracksToPlaylist(self, playlistId, trackIds):
        for i in range(0, len(trackIds), 100):
            batch = trackIds[i:i+100]
            self._withRetry(self.client.playlist_add_items, playlist_id=playlistId, items=batch)


class SpotifyRadio:
    def __init__(self, spotifyConn, config, topTrackIds, topPositions, tooMuchCounts, weightModifier):
        self.conn = spotifyConn
        self.numArtists = config.get("numberOfRadioArtists", 5)
        self.songsPerArtist = config.get("songsPerArtist", 10)
        self.removeByWeight = config.get("removeRadioSongsByWeight", False)
        self.includeInMaster = config.get("includeRadioInMaster", False)
        self.includeDiscover = config.get("includeDiscoverWeeklyInRadio", False)
        self.topTrackIds = topTrackIds
        self.topPositions = topPositions
        self.tooMuchCounts = tooMuchCounts
        self.weightModifier = weightModifier

    def generateRadio(self, masterId):
        radioTracks = []
        attempted = 0

        if self.includeDiscover:
            discoverId = self.conn.getPlaylistIdByName("Discover Weekly")
            if discoverId:
                discoverTracks = self.conn.getPlaylistTracks(discoverId)
                print(f"Including {len(discoverTracks)} tracks from Discover Weekly")
                radioTracks.extend(discoverTracks)

        masterTrackIds = self.conn.getPlaylistTracks(masterId)
        masterInfo = self.conn.getTracksInfo(masterTrackIds)
        # build artistId -> [trackIds] and capture names
        artistMap = {}
        artistNames = {}
        for tid, (_, artistName, artistId) in masterInfo.items():
            if not artistId:
                continue
            artistMap.setdefault(artistId, []).append(tid)
            artistNames[artistId] = artistName

        chosen = random.sample(list(artistMap.keys()), min(self.numArtists, len(artistMap)))
        print(f"Chosen artists for radio: {[artistNames[a] for a in chosen]}")

        for artistId in chosen:
            topResults = self.conn._withRetry(
                self.conn.client.artist_top_tracks,
                artistId,
                country="US"
            )["tracks"]
            count = 0
            for t in topResults:
                attempted += 1
                if count >= self.songsPerArtist:
                    break
                tid = t["id"]
                if self.removeByWeight:
                    weight = 10
                    tmCount = self.tooMuchCounts.get(tid, 0)
                    if tmCount == 1:
                        weight -= 5 * self.weightModifier
                    elif tmCount == 2:
                        weight -= 7 * self.weightModifier
                    elif tmCount >= 3:
                        weight -= 9 * self.weightModifier
                    if tid in self.topPositions:
                        pos = self.topPositions[tid]
                        if pos < 50:
                            weight -= 5 * self.weightModifier
                        elif pos < 100:
                            weight -= 4 * self.weightModifier
                        elif pos < 200:
                            weight -= 3 * self.weightModifier
                    weight = max(0, min(10, weight))
                    if random.random() >= (weight / 10):
                        continue
                radioTracks.append(tid)
                count += 1
            print(f"  {count} tracks added for artist {artistNames[artistId]}")

        filtered = attempted - len(radioTracks)
        print(f"Filtered out {filtered} songs based on weight\n")

        radioId = self.conn.getOrCreatePlaylist("[RX] Radio", description="[RX] Radio generated by script")
        self.conn.clearPlaylist(radioId)
        if radioTracks:
            random.shuffle(radioTracks)
            self.conn.addTracksToPlaylist(radioId, radioTracks)
        print(f"Updated '[RX] Radio' with {len(radioTracks)} tracks")

        if self.includeInMaster:
            currentMaster = set(masterTrackIds)
            toAdd = [tid for tid in radioTracks if tid not in currentMaster]
            if toAdd:
                random.shuffle(toAdd)
                self.conn.addTracksToPlaylist(masterId, toAdd)
            print(f"Appended {len(toAdd)} radio tracks into '[RX] Master'\n")


def loadConfig(configPath="config.json"):
    with open(configPath, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    config = loadConfig()
    weightModifier = config.get("weightModifier", 1)

    spotifyConn = SpotifyConnection(
        clientId="bb8442290b7d4656835f9f2472717ca2",
        clientSecret="",
        redirectUri="http://127.0.0.1:8888/callback",
        scope=(
            "playlist-read-private "
            "playlist-read-collaborative "
            "playlist-modify-public "
            "playlist-modify-private "
            "user-top-read "
            "user-library-read"
        ),
        cachePath=".cache"
    )

    masterId = spotifyConn.getOrCreatePlaylist("[RX] Master", description="Generated by script")
    tooMuchId = spotifyConn.getOrCreatePlaylist("[RX] Songs I Hear Too Much", description="Generated by script")

    topTrackIds = spotifyConn.getUserTopTracks(maxTracks=200)
    topPositions = {tid: idx for idx, tid in enumerate(topTrackIds)}

    tooMuchCounts = {}
    if tooMuchId:
        for tid in spotifyConn.getPlaylistTracks(tooMuchId):
            tooMuchCounts[tid] = tooMuchCounts.get(tid, 0) + 1

    # first: radio
    radio = SpotifyRadio(spotifyConn, config, topTrackIds, topPositions, tooMuchCounts, weightModifier)
    radio.generateRadio(masterId)

    # then: master (verbose)
    rawIds = []
    for name in config["playlistsToInclude"]:
        if name == "Liked Songs":
            rawIds.extend(spotifyConn.getLikedTracks())
        else:
            pid = spotifyConn.getPlaylistIdByName(name)
            if pid:
                rawIds.extend(spotifyConn.getPlaylistTracks(pid))

    allInfo = spotifyConn.getTracksInfo(list(set(rawIds)))
    trackMap = {f"{n} - {a}": (tid, n, a) for tid, (n, a, _) in allInfo.items()}

    selected = []
    print("⎯⎯ Master Weight Decisions ⎯⎯")
    for key, (tid, name, artist) in trackMap.items():
        weight = 10
        count = tooMuchCounts.get(tid, 0)
        if count == 1:
            weight -= 5 * weightModifier
        elif count == 2:
            weight -= 7 * weightModifier
        elif count >= 3:
            weight -= 9 * weightModifier
        if tid in topPositions:
            pos = topPositions[tid]
            if pos < 50:
                weight -= 5 * weightModifier
            elif pos < 100:
                weight -= 4 * weightModifier
            elif pos < 200:
                weight -= 3 * weightModifier
        included = random.random() < (weight / 10)
        if weight != 10 or not included:
            status = "included" if included else "excluded"
            print(f"{name} by {artist}: weight={weight}, {status}")
        if included:
            selected.append(tid)
    print("⎯⎯ End Master Decisions ⎯⎯\n")

    spotifyConn.clearPlaylist(masterId)
    if selected:
        random.shuffle(selected)
        spotifyConn.addTracksToPlaylist(masterId, selected)
    print(f"Updated '[RX] Master' with {len(selected)} tracks")

if __name__ == "__main__":
    main()

import json
import random
import time
import argparse
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
    def __init__(self, spotifyConn, config, topTrackIds, topPositions, tooMuchCounts, weightModifier, artistTooMuch, artistBlacklistNames):
        self.conn = spotifyConn
        self.numArtists = config.get("numberOfRadioArtists", 5)
        self.radioArtistSongs = config.get("radioArtistSongs", 10)
        self.radioArtistRandomSongs = config.get("radioArtistRandomSongs", 5)
        self.removeByWeight = config.get("removeRadioSongsByWeight", False)
        self.includeInMaster = config.get("includeRadioInMaster", False)
        self.includeDiscover = config.get("includeDiscoverWeeklyInRadio", False)
        self.topTrackIds = topTrackIds
        self.topPositions = topPositions
        self.tooMuchCounts = tooMuchCounts
        self.weightModifier = weightModifier
        self.artistTooMuch = artistTooMuch
        self.artistBlacklistNames = artistBlacklistNames
        print("Spotify connection successful.")

    def generateRadio(self, masterId, config):
        print("Generating radio...")
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
            # Skip blacklisted artists (using string matching)
            artistName = artistNames[artistId]
            isBlacklisted = False
            if config.get("artistBlacklist", False):
                for blacklistedName in self.artistBlacklistNames:
                    if blacklistedName.lower() in artistName.lower():
                        isBlacklisted = True
                        break
            
            if isBlacklisted:
                print(f"  Skipping blacklisted artist: {artistName}")
                continue
                
            # Get artist's top tracks
            topResults = self.conn._withRetry(
                self.conn.client.artist_top_tracks,
                artistId,
                country="US"
            )["tracks"]
            
            # Get artist's albums to find more songs for random selection
            albums = self.conn._withRetry(
                self.conn.client.artist_albums,
                artistId,
                album_type="album,single",
                limit=50
            )
            
            # Collect all tracks from albums
            allArtistTracks = []
            for album in albums["items"]:
                albumTracks = self.conn._withRetry(
                    self.conn.client.album_tracks,
                    album["id"]
                )
                for track in albumTracks["items"]:
                    # Only include tracks where this artist is the main artist
                    if track["artists"][0]["id"] == artistId:
                        allArtistTracks.append(track)
            
            # Remove duplicates based on track ID
            uniqueTracks = {}
            for track in allArtistTracks:
                if track["id"] not in uniqueTracks:
                    uniqueTracks[track["id"]] = track
            
            # Sort by popularity (if available) and take top 75%
            sortedTracks = sorted(uniqueTracks.values(), key=lambda x: x.get("popularity", 0), reverse=True)
            top75Percent = sortedTracks[:int(len(sortedTracks) * 0.75)]
            
            print(f"  {artistName}: {len(topResults)} top tracks, {len(top75Percent)} tracks in top 75%")
            
            # First, collect all top songs
            topSongs = []
            for t in topResults:
                if len(topSongs) >= self.radioArtistSongs:
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
                topSongs.append(tid)
                attempted += 1
            
            # Now replace some top songs with random songs
            finalSongs = topSongs.copy()
            random.shuffle(top75Percent)
            
            replacementCount = 0
            for t in top75Percent:
                if replacementCount >= self.radioArtistRandomSongs:
                    break
                tid = t["id"]
                # Skip if already in top songs
                if tid in topSongs:
                    continue
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
                
                # Replace a random top song with this random song
                if finalSongs:
                    replaceIndex = random.randint(0, len(finalSongs) - 1)
                    finalSongs[replaceIndex] = tid
                    replacementCount += 1
                    attempted += 1
            
            radioTracks.extend(finalSongs)
            print(f"  {len(topSongs)} top tracks, {replacementCount} replaced with random tracks for artist {artistName}")

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
    try:
        with open(configPath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def parseArgs():
    parser = argparse.ArgumentParser(description="PlaylistRX - Spotify playlist generator")
    parser.add_argument("--clientId", help="Spotify Client ID")
    parser.add_argument("--clientSecret", help="Spotify Client Secret")
    parser.add_argument("--redirectUri", default="http://127.0.0.1:8888/callback", help="Redirect URI")
    parser.add_argument("--config", default="config.json", help="Config file path")
    parser.add_argument("--weightModifier", type=float, help="Weight modifier value")
    parser.add_argument("--numberOfRadioArtists", type=int, help="Number of radio artists")
    parser.add_argument("--radioArtistSongs", type=int, help="Top songs per radio artist")
    parser.add_argument("--radioArtistRandomSongs", type=int, help="Random songs per radio artist")
    parser.add_argument("--removeRadioSongsByWeight", action="store_true", help="Remove radio songs by weight")
    parser.add_argument("--includeRadioInMaster", action="store_true", help="Include radio in master")
    parser.add_argument("--includeDiscoverWeeklyInRadio", action="store_true", help="Include Discover Weekly in radio")
    parser.add_argument("--playlistsToInclude", nargs="+", help="Playlists to include")
    parser.add_argument("--artistIHearTooMuch", action="store_true", help="Enable artist I hear too much feature")
    parser.add_argument("--splitTheDeck", action="store_true", help="Enable split the deck feature")
    parser.add_argument("--artistBlacklist", action="store_true", help="Enable artist blacklist feature")
    return parser.parse_args()

def main():
    args = parseArgs()
    config = loadConfig(args.config)
    
    # Override config with command line args
    if args.weightModifier is not None:
        config["weightModifier"] = args.weightModifier
    if args.numberOfRadioArtists is not None:
        config["numberOfRadioArtists"] = args.numberOfRadioArtists
    if args.radioArtistSongs is not None:
        config["radioArtistSongs"] = args.radioArtistSongs
    if args.radioArtistRandomSongs is not None:
        config["radioArtistRandomSongs"] = args.radioArtistRandomSongs
    if args.removeRadioSongsByWeight:
        config["removeRadioSongsByWeight"] = True
    if args.includeRadioInMaster:
        config["includeRadioInMaster"] = True
    if args.includeDiscoverWeeklyInRadio:
        config["includeDiscoverWeeklyInRadio"] = True
    if args.playlistsToInclude:
        config["playlistsToInclude"] = args.playlistsToInclude
    if args.artistIHearTooMuch:
        config["artistIHearTooMuch"] = True
    if args.splitTheDeck:
        config["splitTheDeck"] = True
    if args.artistBlacklist:
        config["artistBlacklist"] = True
    
    weightModifier = config.get("weightModifier", 1)
    
    # Get credentials from args or config
    clientId = args.clientId or config.get("clientId")
    clientSecret = args.clientSecret or config.get("clientSecret")
    redirectUri = args.redirectUri or config.get("redirectUri", "http://127.0.0.1:8888/callback")

    spotifyConn = SpotifyConnection(
        clientId=clientId,
        clientSecret=clientSecret,
        redirectUri=redirectUri,
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

    # Build artist-too-much mapping if enabled
    artistTooMuch = set()
    artistBlacklistNames = set()  # Store artist names for string matching
    if config.get("artistIHearTooMuch", False) or config.get("artistBlacklist", False):
        tooMuchTracks = spotifyConn.getPlaylistTracks(tooMuchId) if tooMuchId else []
        if tooMuchTracks:
            # Count actual track occurrences (including duplicates)
            trackOccurrences = {}
            for tid in tooMuchTracks:
                trackOccurrences[tid] = trackOccurrences.get(tid, 0) + 1
            print("Getting track info")
            tooMuchInfo = spotifyConn.getTracksInfo(list(trackOccurrences.keys()))
            artistNameCounts = {}  # Count by artist name
            
            for tid, (trackName, artistName, artistId) in tooMuchInfo.items():
                if artistName:
                    # Count each track the number of times it appears
                    count = trackOccurrences.get(tid, 1)
                    artistNameCounts[artistName] = artistNameCounts.get(artistName, 0) + count
            
            # Tiered system: 3+ for weight reduction, 10+ for blacklist
            if config.get("artistIHearTooMuch", False):
                # For weight reduction, we still need artist IDs for the existing logic
                artistTooMuch = set()
                for tid, (_, artistName, artistId) in tooMuchInfo.items():
                    if artistId and artistNameCounts.get(artistName, 0) >= 3:
                        artistTooMuch.add(artistId)
                        
            if config.get("artistBlacklist", False):
                # For blacklisting, use artist names
                artistBlacklistNames = {name for name, count in artistNameCounts.items() if count >= 10}
            
            if artistTooMuch:
                print(f"Found {len(artistTooMuch)} artists with 3+ songs in 'Songs I Hear Too Much':")
                # Get artist names and counts
                for artistName, count in artistNameCounts.items():
                    if count >= 3:
                        status = " (BLACKLISTED)" if count >= 10 else ""
                        print(f"  {artistName}: {count} songs{status}")
            if artistBlacklistNames:
                print(f"Found {len(artistBlacklistNames)} artists with 10+ songs - BLACKLISTED")
                print(f"Blacklisted artists: {', '.join(artistBlacklistNames)}")

    # first: radio
    radio = SpotifyRadio(spotifyConn, config, topTrackIds, topPositions, tooMuchCounts, weightModifier, artistTooMuch, artistBlacklistNames)
    radio.generateRadio(masterId, config)

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
        # Skip blacklisted artists entirely (using string matching)
        isBlacklisted = False
        if config.get("artistBlacklist", False):
            for blacklistedName in artistBlacklistNames:
                if blacklistedName.lower() in artist.lower():
                    isBlacklisted = True
                    break
        
        if isBlacklisted:
            print(f"{name} by {artist}: BLACKLISTED (10+ songs in 'Songs I Hear Too Much')")
            continue
            
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
                
        # Enhanced artist weight reduction for 6+ songs
        if tid in allInfo and config.get("artistIHearTooMuch", False):
            artistId = allInfo[tid][2]
            if artistId in artistTooMuch:
                # Count how many songs by this artist are in 'Songs I Hear Too Much'
                artistCount = 0
                for trackId, (_, _, trackArtistId) in allInfo.items():
                    if trackArtistId == artistId and trackId in tooMuchCounts:
                        artistCount += tooMuchCounts[trackId]
                
                if artistCount >= 6:
                    weight -= 3 * weightModifier
                    print(f"{name} by {artist}: artist has {artistCount} songs in 'Songs I Hear Too Much', weight reduced by 3")
                elif artistCount >= 3:
                    weight -= 2 * weightModifier
                    print(f"{name} by {artist}: artist has {artistCount} songs in 'Songs I Hear Too Much', weight reduced by 2")
                    
        included = random.random() < (weight / 10)
        if weight != 10 or not included:
            status = "included" if included else "excluded"
            print(f"{name} by {artist}: weight={weight}, {status}")
        if included:
            selected.append(tid)
    print("⎯⎯ End Master Decisions ⎯⎯\n")

    if selected:
        if config.get("splitTheDeck", False):
            print("⎯⎯ Split the Deck ⎯⎯")
            currentMasterTracks = spotifyConn.getPlaylistTracks(masterId)
            
            if currentMasterTracks:
                # Take first 1/4 of current master
                splitPoint = len(currentMasterTracks) // 4
                topOfMaster = currentMasterTracks[:splitPoint]
                print(f"Split point: {splitPoint} tracks (1/4 of {len(currentMasterTracks)})")
                
                # Shuffle the newly selected tracks
                random.shuffle(selected)
                
                # Remove bottom half of shuffled selected tracks
                bottomHalfPoint = len(selected) // 2
                bottomHalfOfMaster = selected[bottomHalfPoint:]
                topHalfOfSelected = selected[:bottomHalfPoint]
                
                # Combine bottom half with top quarter and shuffle together
                combinedForBottom = bottomHalfOfMaster + topOfMaster
                random.shuffle(combinedForBottom)
                
                # Final result: top half of selected + combined shuffled bottom
                selected = topHalfOfSelected + combinedForBottom
                
                print(f"Removed {len(topOfMaster)} tracks from top of current master")
                print(f"Shuffled {len(selected)} newly selected tracks")
                print(f"Removed {len(bottomHalfOfMaster)} tracks from bottom half, combined with top quarter")
                print(f"Final playlist: {len(topHalfOfSelected)} + {len(combinedForBottom)} = {len(selected)} tracks")
            else:
                # No current master, just shuffle normally
                random.shuffle(selected)
                print("No current master tracks found, shuffling normally")
            print("⎯⎯ End Split the Deck ⎯⎯\n")
        else:
            # Normal shuffle
            random.shuffle(selected)
        
        # Clear playlist after we've retrieved the current tracks
        spotifyConn.clearPlaylist(masterId)
        spotifyConn.addTracksToPlaylist(masterId, selected)
    print(f"Updated '[RX] Master' with {len(selected)} tracks")

if __name__ == "__main__":
    print("Starting PlaylistRX")
    main()

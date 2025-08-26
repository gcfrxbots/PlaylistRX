import json
import random
import time
import argparse
import sys
import threading
from datetime import datetime
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
        self.apiCallCount = 0  # Track total API calls

    def _call_with_timeout(self, func, *args, timeout=30, **kwargs):
        """Execute a function with a timeout using threading"""
        result = [None]
        exception = [None]
        
        def target():
            try:
                result[0] = func(*args, **kwargs)
            except Exception as e:
                exception[0] = e
        
        thread = threading.Thread(target=target)
        thread.daemon = True
        thread.start()
        thread.join(timeout)
        
        if thread.is_alive():
            print(f"    API call timed out after {timeout} seconds", file=sys.stderr)
            raise TimeoutError(f"API call timed out after {timeout} seconds")
        
        if exception[0]:
            raise exception[0]
        
        return result[0]

    def _withRetry(self, func, *args, **kwargs):
        maxRetries = 5
        retryCount = 0
        
        while retryCount < maxRetries:
            try:
                result = self._call_with_timeout(func, *args, timeout=60, **kwargs)
                self.apiCallCount += 1  # Count successful API calls
                return result
            except TimeoutError as e:
                retryCount += 1
                errorMsg = f"API call timed out: {str(e)}"
                print(errorMsg, file=sys.stderr)
                if retryCount >= maxRetries:
                    print(f"Max retries ({maxRetries}) reached. Giving up.")
                    raise
                print(f"Retrying... (attempt {retryCount + 1}/{maxRetries})")
            except SpotifyException as e:
                retryCount += 1
                if e.http_status == 429:
                    retryAfter = int(e.headers.get("Retry-After", 1))
                    errorMsg = f"Rate limited by Spotify API. HTTP {e.http_status}. Sleeping {retryAfter}s"
                    print(errorMsg, file=sys.stderr)
                    print(f"Rate limited. Sleeping {retryAfter}s")
                    time.sleep(retryAfter)
                elif e.http_status == 503:
                    errorMsg = f"Spotify API service unavailable. HTTP {e.http_status}. Retrying in 5s"
                    print(errorMsg, file=sys.stderr)
                    print("Service unavailable. Retrying in 5s")
                    time.sleep(5)
                elif e.http_status >= 500:
                    errorMsg = f"Spotify API server error. HTTP {e.http_status}. Retrying in 10s"
                    print(errorMsg, file=sys.stderr)
                    print(f"Server error {e.http_status}. Retrying in 10s")
                    time.sleep(10)
                else:
                    errorMsg = f"Spotify API error: HTTP {e.http_status} - {e.msg}"
                    print(errorMsg, file=sys.stderr)
                    if retryCount >= maxRetries:
                        print(f"Max retries ({maxRetries}) reached. Giving up.")
                        raise
                    print(f"Retrying... (attempt {retryCount + 1}/{maxRetries})")
            except Exception as e:
                retryCount += 1
                errorMsg = f"Unexpected error during Spotify API call: {str(e)}"
                print(errorMsg, file=sys.stderr)
                if retryCount >= maxRetries:
                    print(f"Max retries ({maxRetries}) reached. Giving up.")
                    raise
                print(f"Retrying... (attempt {retryCount + 1}/{maxRetries})")
        
        # If we get here, we've exhausted all retries
        raise Exception(f"Failed after {maxRetries} attempts")

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
        batchNum = 1
        print("Fetching liked tracks...")
        
        while True:
            results = self._withRetry(self.client.current_user_saved_tracks, limit=50, offset=offset)
            items = results.get("items", [])
            if not items:
                break
            for item in items:
                track = item.get("track")
                if track and track.get("id"):
                    trackIds.append(track["id"])
            print(f"  ✓ Batch {batchNum} - {len(items)} tracks (total: {len(trackIds)})")
            offset += len(items)
            batchNum += 1
        print(f"Completed - {len(trackIds)} liked tracks fetched")
        return trackIds

    def getUserTopTracks(self, maxTracks=200, timeRange="medium_term"):
        allIds = []
        totalBatches = (maxTracks + 49) // 50
        print(f"Fetching top {maxTracks} tracks ({timeRange})...")
        
        for offset in range(0, maxTracks, 50):
            batchNum = (offset // 50) + 1
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
            print(f"  ✓ Batch {batchNum}/{totalBatches} - {len(items)} tracks (total: {len(allIds)})")
            if len(items) < limitPerRequest:
                break
        print(f"Completed - {len(allIds)} top tracks fetched")
        return allIds

    def getTracksInfo(self, trackIds):
        info = {}
        totalTracks = len(trackIds)
        print(f"Fetching track info for {totalTracks} tracks...")
        
        # Filter out None, empty, or invalid track IDs
        validTrackIds = [tid for tid in trackIds if tid and isinstance(tid, str) and len(tid) > 0]
        if len(validTrackIds) != totalTracks:
            print(f"Filtered out {totalTracks - len(validTrackIds)} invalid track IDs")
            totalTracks = len(validTrackIds)
        
        if totalTracks == 0:
            print("No valid track IDs to process")
            return info
        
        # Use maximum batch size for efficiency
        batchSize = 50
        for i in range(0, len(validTrackIds), batchSize):
            batch = validTrackIds[i:i+batchSize]
            batchNum = (i // batchSize) + 1
            totalBatches = (totalTracks + batchSize - 1) // batchSize
            
            try:
                results = self._withRetry(self.client.tracks, batch)
                
                processedCount = 0
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
                        processedCount += 1
                print(f"  ✓ Batch {batchNum}/{totalBatches} - {processedCount} tracks")
            except SpotifyException as e:
                errorMsg = f"Error getting track info for batch {batchNum}: HTTP {e.http_status} - {e.msg}"
                print(errorMsg, file=sys.stderr)
                print(f"  ✗ Batch {batchNum}/{totalBatches} - Error: {e.http_status}")
                # Continue with next batch instead of failing completely
                continue
            except Exception as e:
                errorMsg = f"Unexpected error getting track info for batch {batchNum}: {str(e)}"
                print(errorMsg, file=sys.stderr)
                print(f"  ✗ Batch {batchNum}/{totalBatches} - Error: {str(e)}")
                # Continue with next batch instead of failing completely
                continue
        print(f"Completed - {len(info)} tracks processed")
        return info

    def clearPlaylist(self, playlistId):
        self._withRetry(self.client.playlist_replace_items, playlist_id=playlistId, items=[])

    def addTracksToPlaylist(self, playlistId, trackIds):
        totalTracks = len(trackIds)
        totalBatches = (totalTracks + 99) // 100
        print(f"Adding {totalTracks} tracks to playlist...")
        
        for i in range(0, len(trackIds), 100):
            batch = trackIds[i:i+100]
            batchNum = (i // 100) + 1
            self._withRetry(self.client.playlist_add_items, playlist_id=playlistId, items=batch)
            print(f"  ✓ Batch {batchNum}/{totalBatches} - {len(batch)} tracks added")
        print(f"Completed - {totalTracks} tracks added to playlist")

    def getArtistsTopTracks(self, artistIds):
        """Get top tracks for multiple artists in batches"""
        allTopTracks = {}
        totalArtists = len(artistIds)
        print(f"Fetching top tracks for {totalArtists} artists...")
        
        successCount = 0
        errorCount = 0
        
        for i in range(0, len(artistIds), 20):  # Process 20 artists at a time
            batch = artistIds[i:i+20]
            batchNum = (i // 20) + 1
            totalBatches = (totalArtists + 19) // 20
            
            for j, artistId in enumerate(batch):
                try:
                    results = self._withRetry(
                        self.client.artist_top_tracks,
                        artistId,
                        country="US"
                    )
                    allTopTracks[artistId] = results["tracks"]
                    successCount += 1
                except SpotifyException as e:
                    errorMsg = f"Error getting top tracks for artist {artistId}: HTTP {e.http_status} - {e.msg}"
                    print(errorMsg, file=sys.stderr)
                    allTopTracks[artistId] = []
                    errorCount += 1
                except Exception as e:
                    errorMsg = f"Unexpected error getting top tracks for artist {artistId}: {str(e)}"
                    print(errorMsg, file=sys.stderr)
                    allTopTracks[artistId] = []
                    errorCount += 1
            
            print(f"  ✓ Batch {batchNum}/{totalBatches} completed")
        
        print(f"Completed - {successCount} artists processed, {errorCount} errors")
        return allTopTracks

    def getArtistsAlbums(self, artistIds):
        """Get albums for multiple artists in batches"""
        allAlbums = {}
        totalArtists = len(artistIds)
        print(f"Fetching albums for {totalArtists} artists...")
        
        for i in range(0, len(artistIds), 20):  # Process 20 artists at a time
            batch = artistIds[i:i+20]
            batchNum = (i // 20) + 1
            totalBatches = (totalArtists + 19) // 20
            print(f"  Batch {batchNum}/{totalBatches} ({len(batch)} artists)")
            
            for j, artistId in enumerate(batch):
                try:
                    results = self._withRetry(
                        self.client.artist_albums,
                        artistId,
                        album_type="album,single",
                        limit=50
                    )
                    allAlbums[artistId] = results["items"]
                    print(f"    ✓ Artist {j+1}/{len(batch)} - {len(results['items'])} albums")
                except SpotifyException as e:
                    errorMsg = f"Error getting albums for artist {artistId}: HTTP {e.http_status} - {e.msg}"
                    print(errorMsg, file=sys.stderr)
                    print(f"    ✗ Artist {j+1}/{len(batch)} - Error: {e.http_status}")
                    allAlbums[artistId] = []
                except Exception as e:
                    errorMsg = f"Unexpected error getting albums for artist {artistId}: {str(e)}"
                    print(errorMsg, file=sys.stderr)
                    print(f"    ✗ Artist {j+1}/{len(batch)} - Error: {str(e)}")
                    allAlbums[artistId] = []
        print(f"Completed fetching albums for {len(allAlbums)} artists")
        return allAlbums

    def getAlbumsTracks(self, albumIds):
        """Get tracks for multiple albums in batches"""
        allAlbumTracks = {}
        totalAlbums = len(albumIds)
        print(f"Fetching tracks for {totalAlbums} albums...")
        
        for i in range(0, len(albumIds), 20):  # Process 20 albums at a time
            batch = albumIds[i:i+20]
            batchNum = (i // 20) + 1
            totalBatches = (totalAlbums + 19) // 20
            print(f"  Batch {batchNum}/{totalBatches} ({len(batch)} albums)")
            
            for j, albumId in enumerate(batch):
                try:
                    results = self._withRetry(self.client.album_tracks, albumId)
                    allAlbumTracks[albumId] = results["items"]
                    print(f"    ✓ Album {j+1}/{len(batch)} - {len(results['items'])} tracks")
                except SpotifyException as e:
                    errorMsg = f"Error getting tracks for album {albumId}: HTTP {e.http_status} - {e.msg}"
                    print(errorMsg, file=sys.stderr)
                    print(f"    ✗ Album {j+1}/{len(batch)} - Error: {e.http_status}")
                    allAlbumTracks[albumId] = []
                except Exception as e:
                    errorMsg = f"Unexpected error getting tracks for album {albumId}: {str(e)}"
                    print(errorMsg, file=sys.stderr)
                    print(f"    ✗ Album {j+1}/{len(batch)} - Error: {str(e)}")
                    allAlbumTracks[albumId] = []
        print(f"Completed fetching tracks for {len(allAlbumTracks)} albums")
        return allAlbumTracks

    def getPlaylistsTracks(self, playlistNames):
        """Get tracks for multiple playlists in batches"""
        allPlaylistTracks = {}
        totalPlaylists = len(playlistNames)
        print(f"Fetching tracks from {totalPlaylists} playlists...")
        
        for i, name in enumerate(playlistNames):
            print(f"  Playlist {i+1}/{totalPlaylists}: {name}")
            if name == "Liked Songs":
                tracks = self.getLikedTracks()
                allPlaylistTracks[name] = tracks
                print(f"    ✓ Liked Songs - {len(tracks)} tracks")
            else:
                playlistId = self.getPlaylistIdByName(name)
                if playlistId:
                    tracks = self.getPlaylistTracks(playlistId)
                    allPlaylistTracks[name] = tracks
                    print(f"    ✓ {name} - {len(tracks)} tracks")
                else:
                    allPlaylistTracks[name] = []
                    print(f"    ✗ {name} - Not found")
        print(f"Completed fetching tracks from {len(allPlaylistTracks)} playlists")
        return allPlaylistTracks

    def isTitleExcluded(self, title, excludedWords):
        """Check if a song title contains any excluded words"""
        if not excludedWords or not title:
            return False
        titleLower = title.lower()
        for word in excludedWords:
            if word.lower() in titleLower:
                return True
        return False
    
    def getApiCallCount(self):
        """Return the total number of API calls made"""
        return self.apiCallCount
    
    def resetApiCallCount(self):
        """Reset the API call counter"""
        self.apiCallCount = 0


class SpotifyRadio:
    def __init__(self, spotifyConn, config, topTrackIds, topPositions, tooMuchCounts, weightModifier, artistTooMuch, artistBlacklistNames):
        self.conn = spotifyConn
        self.numArtists = config.get("numberOfRadioArtists", 5)
        self.radioArtistSongs = config.get("radioArtistSongs", 10)
        self.removeByWeight = config.get("removeRadioSongsByWeight", False)
        self.includeInMaster = config.get("includeRadioInMaster", False)
        self.includeDiscover = config.get("includeDiscoverWeeklyInRadio", False)
        self.excludedWords = config.get("excludedWords", [])
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

        # Only get top tracks - one API call per artist
        print("Fetching top tracks for artists...")
        allTopTracks = self.conn.getArtistsTopTracks(chosen)

        for i, artistId in enumerate(chosen):
            artistName = artistNames[artistId]
            print(f"Processing artist {i+1}/{len(chosen)}: {artistName}")
            
            # Skip blacklisted artists (using string matching)
            isBlacklisted = False
            if config.get("artistBlacklist", False):
                for blacklistedName in self.artistBlacklistNames:
                    if blacklistedName.lower() in artistName.lower():
                        isBlacklisted = True
                        break
            
            if isBlacklisted:
                print(f"  Skipping blacklisted artist: {artistName}")
                continue
                
            # Get artist's top tracks (max 10 from API)
            topResults = allTopTracks.get(artistId, [])
            print(f"  {artistName}: {len(topResults)} top tracks available")
            
            # Filter and select songs from top tracks only
            eligibleSongs = []
            for t in topResults:
                tid = t["id"]
                title = t["name"]
                
                # Skip songs with excluded words in title
                if self.conn.isTitleExcluded(title, self.excludedWords):
                    print(f"    Skipping '{title}' - contains excluded word")
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
                
                eligibleSongs.append(tid)
                attempted += 1
            
            # Select songs randomly from eligible top tracks
            if eligibleSongs:
                numToSelect = min(self.radioArtistSongs, len(eligibleSongs))
                if numToSelect >= len(eligibleSongs):
                    # Select all if we want more than available
                    selectedSongs = eligibleSongs
                else:
                    # Randomly select the specified number
                    selectedSongs = random.sample(eligibleSongs, numToSelect)
                
                radioTracks.extend(selectedSongs)
                print(f"  Selected {len(selectedSongs)} tracks from {len(eligibleSongs)} eligible tracks for {artistName}")
            else:
                print(f"  No eligible tracks found for {artistName}")

        filtered = attempted - len(radioTracks)
        print(f"Filtered out {filtered} songs based on weight\n")

        currentDate = datetime.now().strftime("%Y-%m-%d")
        radioId = self.conn.getOrCreatePlaylist("[RX] Radio", description=f"[RX] Radio generated by script - {currentDate}")
        self.conn.clearPlaylist(radioId)
        if radioTracks:
            random.shuffle(radioTracks)
            self.conn.addTracksToPlaylist(radioId, radioTracks)
        print(f"Updated '[RX] Radio' with {len(radioTracks)} tracks")


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
    parser.add_argument("--artistBlacklist", action="store_true", help="Enable artist blacklist feature")
    parser.add_argument("--masterSongs", type=int, help="Maximum number of songs in master playlist before adding radio")
    parser.add_argument("--excludedWords", nargs="+", help="Words to exclude from song titles")
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
    if args.artistBlacklist:
        config["artistBlacklist"] = True
    if args.masterSongs is not None:
        config["masterSongs"] = args.masterSongs
    if args.excludedWords:
        config["excludedWords"] = args.excludedWords
    
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

    currentDate = datetime.now().strftime("%Y-%m-%d")
    masterId = spotifyConn.getOrCreatePlaylist("[RX] Master", description=f"Generated by script - {currentDate}")
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
        # Use the tracks we already fetched for tooMuchCounts
        if tooMuchId:
            tooMuchTracks = spotifyConn.getPlaylistTracks(tooMuchId)
            if tooMuchTracks:
                # Count actual track occurrences (including duplicates)
                trackOccurrences = {}
                for tid in tooMuchTracks:
                    trackOccurrences[tid] = trackOccurrences.get(tid, 0) + 1
                print("Getting track info for 'Songs I Hear Too Much'")
                trackIds = list(trackOccurrences.keys())
                print(f"Track IDs to process: {len(trackIds)}")
                if trackIds:
                    print(f"First few track IDs: {trackIds[:5]}")
                tooMuchInfo = spotifyConn.getTracksInfo(trackIds)
                
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
    print("Fetching playlist tracks in batches...")
    allPlaylistTracks = spotifyConn.getPlaylistsTracks(config["playlistsToInclude"])
    
    rawIds = []
    for name, trackIds in allPlaylistTracks.items():
        rawIds.extend(trackIds)

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
        # Shuffle the selected tracks first
        random.shuffle(selected)
        
        # Apply masterSongs limit BEFORE adding radio
        masterSongs = config.get("masterSongs", 1000) # Default to 1000 if not specified
        if len(selected) > masterSongs:
            print(f"Selected tracks exceed masterSongs ({masterSongs}). Truncating to {masterSongs} tracks.")
            selected = selected[:masterSongs]
        
        # Get radio tracks if enabled and shuffle them INTO master
        finalTracks = selected.copy()
        if config.get("includeRadioInMaster", False):
            radioId = spotifyConn.getPlaylistIdByName("[RX] Radio")
            if radioId:
                radioTracks = spotifyConn.getPlaylistTracks(radioId)
                if radioTracks:
                    print(f"Integrating {len(radioTracks)} radio tracks into master playlist")
                    finalTracks.extend(radioTracks)
                    # Final shuffle to integrate radio tracks throughout the playlist
                    random.shuffle(finalTracks)
                    print(f"Final '[RX] Master' contains {len(selected)} master + {len(radioTracks)} radio = {len(finalTracks)} tracks total")
                else:
                    print("No radio tracks found to add to master")
            else:
                print("Radio playlist not found")
        
        # Clear playlist and add all tracks (master + radio, shuffled together)
        spotifyConn.clearPlaylist(masterId)
        spotifyConn.addTracksToPlaylist(masterId, finalTracks)
        print(f"Updated '[RX] Master' with {len(finalTracks)} tracks")
    else:
        print("No tracks selected for master playlist")
    
    # Print total API calls made
    totalApiCalls = spotifyConn.getApiCallCount()
    print(f"\nTotal API calls made: {totalApiCalls}")

if __name__ == "__main__":
    print("Starting PlaylistRX")
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        errorMsg = f"Fatal error: {str(e)}"
        print(errorMsg, file=sys.stderr)
        print(f"Fatal error: {e}")
        sys.exit(1)


This is a playlist editor for Spotify that is designed to make the listener less sick of music, specifically to avoid Spotify's crappy shuffle algorithm. 

All playlists generated or modified by PlaylistRX are prefaced with [RX] in the playlist name. PlaylistRX does not edit any other playlists (only reads them). 

The script generates a "Master" playlist, a "Radio" playlist, and a "Songs I Hear Too Much" playlist.

The script uses a "weight" system to shuffle songs in and out of the Master playlist. It combines all the songs in the specified playlists into one playlist, then gives each songs a weight of 10. Songs have a Weight out of 10 chance of being included in Master (so a song with a weight of 6 has a 60% chance of being included)
Songs that are in the user's top 200 most listened to songs have points subtracted from their weight (based on how much they're listened to). Songs in the "Songs I Hear Too Much" playlist have -5 to their weight, or -7 if added to the playlist twice, and so on. 

PlaylistRX also generates a Radio playlist, based on the Master. Once the Master has been generated, the radio picks numberOfRadioArtists (default 100) songs from Master, all with different artists. It then selects the top songsPerArtist (default 10) songs from each artist, and adds the result to the Radio playlist. 

Below are the explanations for each setting in config.json, which includes many various features. 

clientId - Spotify Developer Application Client ID
clientSecret - Spotify Developer Application Client Secret
playlistsToInclude - List of all playlists to be shuffled into Master. Use "Liked Songs" to include Liked Songs, and "Discover Weekly" to include that.
weightModifier - Default 1, increase to remove more songs you might be sick of.
numberOfRadioArtists - Number of artists to pick from Master, to pick songs from to add to Radio
radioArtistSongs - How many of each artist's top songs should be added to Radio
radioArtistRandomSongs - Randomly replace this many songs per artist in Radio with random songs by the artist (Picks from their top 75% of songs to avoid crappy ones)
removeRadioSongsByWeight - Should it remove songs you hear too much from the radio too?
includeRadioInMaster - Should it shuffle the radio into Master?
includeDiscoverWeeklyInRadio - Should it shuffle your Discover Weekly into Radio?
artistIHearTooMuch - If 3 or more songs by the same artist are in your "Songs I Hear Too Much" playlist, -3 weight of ALL songs by that artist. Scales up with more songs added.
artistBlacklist - If 10 or more songs by the same artist are in "Songs I Hear Too Much", that artist is blacklisted and will not be added to any RX Playlist.
splitTheDeck - Shuffles the top quarter of songs in current Master into the bottom half of new Master- Prevents repeats if you listen from the top of the playlist.
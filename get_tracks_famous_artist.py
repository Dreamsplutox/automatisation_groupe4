#imports
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import time
import datetime
import os



# ### Get only usefull playlists for the current year (current year + "top" in their name) 
def getUsefullPlaylists(dictPlayLists):
    ok_playlists = []
    
    for i in range(0, len(dictPlayLists)): 
        try:
            if "Top" in dictPlayLists[i]["playListName"] and str(current_year) in dictPlayLists[i]["playListName"]:
                ok_playlists.append(dictPlayLists[i])
        except KeyboardInterrupt:
            print("keyboard interrupt, leaving")
            os._exit(0)
        except:
            print("an error has occured in get usefull playlist")
            continue
        
    return ok_playlists

# ### Get all track ids (+ kill dupplicates)
def getTrackIDs(user, playlist_id):
    ids = []
    playlist = sp.user_playlist(user, playlist_id)
    for item in playlist['tracks']['items']:
        try:
            track = item['track']
            ids.append(track['id'])
        except KeyboardInterrupt:
            print("keyboard interrupt, leaving")
            os._exit(0)
        except:
            print("an error has occured in get track ids")
            continue
    return ids


# ### Get usefull data (artists ids and names) in all the tracks + kill dupplicate artist
def getTrackFeatures(id):
    meta = sp.track(id)
    features = sp.audio_features(id)

    # meta²
    name = meta['name']
    album = meta['album']['name']
    artist = meta['album']['artists'][0]['name']
    artist_id = meta['album']['artists'][0]['id']
    release_date = meta['album']['release_date']
    length = meta['duration_ms']
    popularity = meta['popularity']

    # features

    track = [name, album, artist, artist_id, release_date, length, popularity]
    return track


if __name__ == "__main__":
    #credentials
    clientId= "758b9e90346649409515e5705043464f"
    clientSecret="4130541e7efd4a1c912ac4571909484b"
    client_credentials_manager = SpotifyClientCredentials(clientId, clientSecret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)


    # ### If we are in january, set the year to current year -  1
    current_year = datetime.date.today().year
    current_month = datetime.date.today().month
    if current_month == 1 :
        current_year = current_year - 1


    # ### First get as many playlist as possible (let's say, 200)

    dictPlayLists = []
    for i in range(0,60):
        try:
            track_results = sp.search(q='year:'+str(current_year), type='playlist', limit=1,offset=i)
            tmpDict = None
            tmpDict = {
                'idPlayList':track_results['playlists']['items'][0]['id'],
                'playListName':track_results['playlists']['items'][0]['name']
            }
            dictPlayLists.append(tmpDict)
        except KeyboardInterrupt:
            print("keyboard interrupt, leaving")
            os._exit(0)
        except:
            print("an error has occured in dict playlist")

    dictPlayLists = getUsefullPlaylists(dictPlayLists)

    ids = []

    for elem in dictPlayLists:
        try:
            ids += getTrackIDs(elem["playListName"], elem["idPlayList"])
        except KeyboardInterrupt:
             print("keyboard interrupt, leaving")
             os._exit(0)
        except:
            print("an error has occured in get usefull playlists")
            continue

    #ids = getTrackIDs('Top Hits 2020', '53w0lVHBw0m4eEz54yN8FH')


    # kill dupplicates
    ids = list(dict.fromkeys(ids))

    # loop over track ids 
    tracks = []
    all_artist_ids = []

    for i in range(len(ids)):
        try:
            #time.sleep(.5)
            track = getTrackFeatures(ids[i])
            # check if id of the artist is already here, if that's case don't append the track
            if track[3] not in all_artist_ids:
                # don't add
                all_artist_ids.append(track[3])
                tracks.append(track)
        except KeyboardInterrupt:
            print("keyboard interrupt, leaving")
            os._exit(0)
        except:
            print("an error has occured, go to next track (collect artist phase)")
            continue   

    # ### CREATE / ( maybe MERGE later) CSV with what we have
    # create dataset
    df = pd.DataFrame(tracks, columns = ['name', 'album', 'artist','artist_id', 'release_date', 'length', 'popularity'])

    artist_ids = df["artist_id"].tolist()
    artist_names = df["artist"].tolist()

    #init dict track_artists ==> will contain all songs of famous artists
    track_artists = []

    for i in range(0, len(artist_ids)):
        tracks = sp.artist_top_tracks(artist_ids[i])
        for track in tracks['tracks'] :
            try:
                track_artists.append([artist_ids[i], artist_names[i], track['name'], track['album']['release_date'], track['popularity']])
            except KeyboardInterrupt:
                print("keyboard interrupt, leaving")
                os._exit(0)
            except:
                print("an error has occured, go to next track")
                continue

    # ### Create the dataset
    df = pd.DataFrame(track_artists, columns = ['artist_id', "artist_name", 'track_name', 'track_release_date', 'track_popularity'])
    os.system("")
    df.to_csv("/user/iabd2_group4/test_track_artists_"+str(datetime.date.today())+".csv", sep = ';', encoding="utf-8-sig", index=False)
    #df.to_csv("spotify_famous_artists.csv", sep = ',', encoding="utf-8-sig", index=False)

    f = open("/user/iabd2_group4/demofile2.txt", "w")
    f.write("Now the file has more content!")
    f.close()

    #HDFS part
    source = "test_track_artists_"+str(datetime.date.today())+".csv"
    destination = "/user/iabd2_group4"
    command = "hdfs dfs -copyFromLocal " + source + " " + destination
    print(command)
    os.system(command)

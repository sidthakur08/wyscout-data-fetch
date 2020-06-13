from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen,urlretrieve
from zipfile import ZipFile, is_zipfile
import pandas as pd
import os

data_files = {
    'players':'https://ndownloader.figshare.com/files/15073721', #json
    'teams':'https://ndownloader.figshare.com/files/15073697', #json
    'coaches':'https://ndownloader.figshare.com/files/15073868', #json
    'competitions':'https://ndownloader.figshare.com/files/15073685', #json
    'matches':'https://ndownloader.figshare.com/files/14464622', #zip containing json
    'events':'https://ndownloader.figshare.com/files/14464685' #zip containing json
}

# to extract data from the link and store it in json format
for url in data_files.values():
    print("Getting data from "+url)
    urlopn = urlopen(url).geturl()
    path = Path(urlparse(urlopn).path)
    file_name = path.name
    file_local,_ = urlretrieve(urlopn,file_name)
    if is_zipfile(file_local):
        with ZipFile(file_local) as zip_file:
            zip_file.extractall()

# to get rid of any unicode character and put it in a normal string format
def read_json_file(file_name):
    with open(file_name,'rb') as json_file:
        return BytesIO(json_file.read()).getvalue().decode('unicode_escape')

print("Reading and Converting json to dataframe for teams")
# reading input for teams
json_teams = read_json_file('teams.json')
df_teams = pd.read_json(json_teams)
df_teams.to_csv('teams.csv',index=False)

print("Reading and Converting json to dataframe for players")
# reading input for players
json_players = read_json_file('players.json')
df_players = pd.read_json(json_players)
df_players.to_csv('players.csv',index=False)

print("Reading and Converting json to dataframe for competitions")
# reading input for competitions
json_comp = read_json_file('competitions.json')
df_comp = pd.read_json(json_comp)
df_comp.to_csv('competitions.csv',index=False)

print("Reading and Converting json to dataframe for coaches")
# reading input for coaches
json_coach = read_json_file('coaches.json')
df_coach = pd.read_json(json_coach)
df_coach.to_csv('coaches.csv',index=False)

competitions = ['England','France','Germany',
                'Italy','Spain','World Cup',
                'European Championship']

print("Reading and Converting json to dataframe for matches")
# reading matches from different competitions
comp_name = [str('matches_'+comp.replace(' ','_')) for comp in competitions]
for c in comp_name:
    print('Getting competition: '+c)
    filename = f'{c}/{c}.csv'
    json_matches = read_json_file(c+'.json')
    df_matches = pd.read_json(json_matches)
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))
    df_matches.to_csv(filename,index=False)

print("Reading and Converting json to dataframe for events")
# reading events from different matches from different competitions
event_name = [str('events_'+i.replace(' ','_')) for i in competitions]

for e in event_name:
    print("Getting competition: "+e)
    if e == 'events_England':
        json_events = read_json_file(e+'.json')
        df_events = pd.read_json(json_events)
        df_events_matches = df_events.groupby('matchId',as_index=False)
        for m_id,df_events_match in df_events_matches:
            print("Getting match: "+str(m_id)+" from this competition")
            filename = f'{e}/{m_id}.csv'
            if not os.path.exists(os.path.dirname(filename)):
                os.makedirs(os.path.dirname(filename))
            df_events_match.to_csv(filename,index=False)
    elif e == 'events_France':
        json_events = read_json_file(e+'.json')
        df_events = pd.read_json(json_events)
        df_events_matches = df_events.groupby('matchId',as_index=False)
        for m_id,df_events_match in df_events_matches:
            print("Getting match: "+str(m_id)+" from this competition")
            filename = f'{e}/{m_id}.csv'
            if not os.path.exists(os.path.dirname(filename)):
                os.makedirs(os.path.dirname(filename))
            df_events_match.to_csv(filename,index=False)
    elif e == 'events_Germany':
        json_events = read_json_file(e+'.json')
        df_events = pd.read_json(json_events)
        df_events_matches = df_events.groupby('matchId',as_index=False)
        for m_id,df_events_match in df_events_matches:
            print("Getting match: "+str(m_id)+" from this competition")
            filename = f'{e}/{m_id}.csv'
            if not os.path.exists(os.path.dirname(filename)):
                os.makedirs(os.path.dirname(filename))
            df_events_match.to_csv(filename,index=False)
    elif e == 'events_Italy':
        json_events = read_json_file(e+'.json')
        df_events = pd.read_json(json_events)
        df_events_matches = df_events.groupby('matchId',as_index=False)
        for m_id,df_events_match in df_events_matches:
            print("Getting match: "+str(m_id)+" from this competition")
            filename = f'{e}/{m_id}.csv'
            if not os.path.exists(os.path.dirname(filename)):
                os.makedirs(os.path.dirname(filename))
            df_events_match.to_csv(filename,index=False)
    elif e == 'events_Spain':
        json_events = read_json_file(e+'.json')
        df_events = pd.read_json(json_events)
        df_events_matches = df_events.groupby('matchId',as_index=False)
        for m_id,df_events_match in df_events_matches:
            print("Getting match: "+str(m_id)+" from this competition")
            filename = f'{e}/{m_id}.csv'
            if not os.path.exists(os.path.dirname(filename)):
                os.makedirs(os.path.dirname(filename))
            df_events_match.to_csv(filename,index=False)
    elif e == 'events_World_Cup':
        json_events = read_json_file(e+'.json')
        df_events = pd.read_json(json_events)
        df_events_matches = df_events.groupby('matchId',as_index=False)
        for m_id,df_events_match in df_events_matches:
            print("Getting match: "+str(m_id)+" from this competition")
            filename = f'{e}/{m_id}.csv'
            if not os.path.exists(os.path.dirname(filename)):
                os.makedirs(os.path.dirname(filename))
            df_events_match.to_csv(filename,index=False)
    elif e == 'events_European_Championship':
        json_events = read_json_file(e+'.json')
        df_events = pd.read_json(json_events)
        df_events_matches = df_events.groupby('matchId',as_index=False)
        for m_id,df_events_match in df_events_matches:
            print("Getting match: "+str(m_id)+" from this competition")
            filename = f'{e}/{m_id}.csv'
            if not os.path.exists(os.path.dirname(filename)):
                os.makedirs(os.path.dirname(filename))
            df_events_match.to_csv(filename,index=False)
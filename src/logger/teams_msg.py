import os

import pymsteams

myTeamsMessage = pymsteams.connectorcard(os.environ['TEAMS_WEBHOOK_URL'])


def send_teams_msg(msg: str):
    # TODO: 꾸미기
    myTeamsMessage.text(msg)
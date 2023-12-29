import json

def lambda_handler(event, context):

    event = {
                "danhsach":[
                    {"org": "google", "next":"", "hasNextPage": ""},
                    {"org": "microsoft", "next":"", "hasNextPage": ""}
                ]   
            }
    
    
    return event

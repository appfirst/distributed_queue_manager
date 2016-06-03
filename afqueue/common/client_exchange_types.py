DIRECT = "1"
FANOUT = "2"
TOPIC = "3"

def convert_enum_string_to_name(value):
    if value == "1":
        return "Direct"
    elif value == "2":
        return "Fanout"
    elif value == "3":
        return "Topic"

def convert_name_to_enum_string(value):
    if value == "Direct":
        return "1"
    elif value == "Direct":
        return "2"
    elif value == "Topic":
        return "3"
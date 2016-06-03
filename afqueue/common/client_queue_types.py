DISTRIBUTED = "0"
ORDERED = "1"

def convert_enum_string_to_name(value):
    if value == "0":
        return "Distributed"
    elif value == "1":
        return "Ordered"
    else:
        return "Distributed" 

def convert_name_to_enum_string(value):
    if value == "Distributed":
        return "0"
    elif value == "Ordered":
        return "1"
    else:
        return "0" 

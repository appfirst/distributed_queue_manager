
class NetworkUtilities():

    @staticmethod
    def get_interface_ip_address(interface_name=None):    
        """
        Returns the IP address for the interface specified on the local machine.
        If no interface is specified, will return the first IP address which starts with "10."
        If IP address starting with "10." is found, will return the first IP address which starts with "192."
        If IP address starting with "192." is found, will return the first IP address which does not start with "127."
        """    

        # Get the IP information.
        import subprocess
        raw_result = subprocess.getoutput("/sbin/ifconfig")
        result_list = raw_result.split("\n")
        
        # Parse differently depending on whether or not "flags" appears in the result.
        get_next_key = True
        current_interface_name = None
        interface_name_list = list()
        interface_to_ip_dict = dict()
        
        if "flags" in raw_result:    
            for result in result_list:
                if ord(result[0]) != 9:
                    current_interface_name = result.split(":")[0]
                    interface_name_list.append(current_interface_name)
                else:
                    inet = result.split("inet ")
                    if len(inet) > 1:
                        interface_to_ip_dict[current_interface_name] = inet[1].split()[0]    
        else:        
            for result in result_list:
                if len(result) == 0:
                    get_next_key = True
                else:
                    values = result.split()
                    if get_next_key == True:
                        current_interface_name = values[0]
                        get_next_key = False
                    elif current_interface_name not in interface_name_list:
                        interface_name_list.append(current_interface_name)
                        if len(values) > 1 and "addr:" in values[1]:
                            interface_to_ip_dict[current_interface_name] = values[1][5:]
        
        if interface_name == None:
            for interface_name_test in interface_name_list:
                if interface_name_test in interface_to_ip_dict and interface_to_ip_dict[interface_name_test].startswith("10."):
                    interface_name = interface_name_test
                    break
                
        if interface_name == None:
            for interface_name_test in interface_name_list:
                if interface_name_test in interface_to_ip_dict and interface_to_ip_dict[interface_name_test].startswith("192."):
                    interface_name = interface_name_test
                    break
        
        if interface_name == None:
            for interface_name_test in interface_name_list:
                if interface_name_test in interface_to_ip_dict and interface_to_ip_dict[interface_name_test].startswith("127.") == False:
                    interface_name = interface_name_test
                    break
                
        return interface_to_ip_dict[interface_name]

 
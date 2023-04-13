import email
import os
import time
from pathlib import PureWindowsPath
import pandas as pd
import quotequail


def always_posix(path):
    """
    On any system (Windows or Posix)
    take a path in any format (Windows or Posix)
    and always return a Posix path.
    """
    return PureWindowsPath(
        os.path.normpath(PureWindowsPath(path).as_posix())
    ).as_posix()


def clean_address_fields(f):
    """
    input: string with a raw email address field (From, To, Cc, Bcc) containing from 0 to many addresses
    output: list with just email addresses, stripped of owner names, extra characters, etc.

    An empty string returns an empty list.
    """
    if len(f) == 0:
        return []
    fl = f.split(sep=",")
    for i in range(len(fl)):
        a = fl[i].strip()
        fl[i] = email.utils.parseaddr(a)[1]
    return fl


def ingest_emails(args):
    """
    input: base email directory + a top level folder in basedir
    output: dataframe with all emails parsed out

    Input is a single tuple containing all arguments.
    """
    basedir, top_level_folder = args
    address_headers = ["From", "To", "Cc", "Bcc"]
    non_address_headers = ["Date", "Subject"]
    messages_list = []
    full_top_path = os.path.normpath(basedir + "/" + top_level_folder)
    for path_tuple in os.walk(full_top_path):
        mail_folder = os.path.relpath(path_tuple[0], full_top_path)
        for message_file in path_tuple[2]:
            message_path = os.path.normpath(path_tuple[0] + "/" + message_file)
            with open(message_path, "r") as message_file_pointer:
                message_object = email.message_from_file(message_file_pointer)

            message_dict = {}
            message_dict["Top_Level_Folder"] = top_level_folder
            message_dict["Mail_folder"] = always_posix(mail_folder)
            message_dict["Message_File"] = int(float(message_file))

            # absence of values from fields should have a uniform representation
            # missing address fields will get an empty list
            for k in address_headers:
                message_dict[k] = str(clean_address_fields(""))
            # missing non-address fields will get an empty string
            for k in non_address_headers:
                message_dict[k] = ""

            for it in message_object.items():
                if it[0] == "Date":
                    message_dict["Date"] = time.mktime(email.utils.parsedate(it[1]))
                if it[0] == "Subject":
                    message_dict["Subject"] = it[1]
                for k in address_headers:
                    if it[0] == k:
                        message_dict[k] = str(clean_address_fields(it[1]))

            message_dict["Body"] = message_object.get_payload()
            message_dict["Body_Message"] = ""
            message_dict["Body_Quoted"] = ""
            for q in quotequail.quote(message_dict["Body"]):
                if q[0] == True:
                    message_dict["Body_Message"] += q[1]
                elif q[0] == False:
                    message_dict["Body_Quoted"] += q[1]

            messages_list.append(pd.DataFrame(data=message_dict, index=[0]))

    return pd.concat(messages_list, ignore_index=False)

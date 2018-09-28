import sys
import string
import subprocess
import json
from random import choice
from os.path import isfile
from time import time

DEFAULT_RAND_STR_SIZE = 10
DEFAULT_RAND_STR_CHARS = string.lowercase + string.uppercase + string.digits

USER_PREFIX = "census_user_"
USER_JSON = "census_data_users.json"
HTPASSWDFILE = "../.htpasswd-data-release"
MONTH_IN_S = 30*24*3600  # month as seconds


def read_json(json_path):
    return json.load(open(json_path))


def dump_as_json(obj, json_path):
    with open(json_path, 'w') as f:
        json.dump(obj, f)


def rand_str(size=DEFAULT_RAND_STR_SIZE, chars=DEFAULT_RAND_STR_CHARS):
    """Return random string given a size and character space."""
    return ''.join(choice(chars) for _ in range(size))


def get_expiry():
    return int(time() + MONTH_IN_S)


def generate_username():
    return USER_PREFIX + rand_str(6)


def generate_password():
    return rand_str(16)


def delete_from_htpasswd(username):
    print subprocess.call(["htpasswd", "-D", HTPASSWDFILE, username])


def update_user_expiry_db(username):
    users = {}
    if isfile(USER_JSON):
        users = read_json(USER_JSON)
    users[username] = get_expiry()
    dump_as_json(users, USER_JSON)
    print "Added: " + username + " Current # users", len(users)


def revoke_expired_accounts():
    expired_usernames = set()
    users = read_json(USER_JSON)
    now = time()
    for username, expiry_time in users.iteritems():
        if now > expiry_time:
            expired_usernames.add(username)
    for expired_username in expired_usernames:
        print "Revoking expired account", expired_username
        users.pop(expired_username)
        delete_from_htpasswd(expired_username)
    dump_as_json(users, USER_JSON)


def add_new_user():
    username = generate_username()
    update_user_expiry_db(username)
    password = generate_password()
    print "Username: %s\nPassword: %s" % (username, password)
    subprocess.call(["htpasswd", "-b", HTPASSWDFILE, username, password])


# USAGE
# Add a new user:
#   python manage_users add

# Remove expired users
#   python manage_users revoke


if __name__ == '__main__':
    command = sys.argv[1]
    if command == "revoke":
        revoke_expired_accounts()
    elif command == "add":
        add_new_user()
    else:
        print "Unknown command"

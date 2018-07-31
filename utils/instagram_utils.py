import instaloader
import time
import csv


def get_profile_instagram(name, output_file):
    # Create an Instaloader object
    IL = instaloader.Instaloader()

    print("Getting {}'s Instagram profile...".format(name))
    start = time.time()

    # Use the Instaloader library function to get the profile of the designated user
    profile = instaloader.Profile.from_username(IL.context, name)

    # Extract the appropriate data fields
    columns = ['Username', 'User ID', 'Media Count', 'Followers Count', 'Follow Count', 'Private']
    data = [profile.username, profile.userid, profile.mediacount, profile.followers, profile.followees, profile.is_private]

    # Write the data to the output
    with open(output_file, 'a') as wf:
        writer = csv.writer(wf)
        writer.writerow(columns)
        writer.writerow(data)

    end = time.time()
    print("Successfully retrieved {}'s Instagram profile in {} seconds!\n".format(name, end - start))
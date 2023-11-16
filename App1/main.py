from flask import (
    Flask,
    session,
    escape,
    redirect,
    render_template,
    request,
    url_for,
    flash,
)
from google.cloud import datastore
from google.cloud import storage
from google.cloud import logging as gcloud_logging
from PIL import Image
from datetime import datetime as DateTime
from datetime import timezone
import datetime
import logging
import os

# project_id = "s3814655-oua23sp3-task-1"
# App URL: https://s3814655-oua23sp3-task-1.ts.r.appspot.com
# Service account: s3814655-oua23sp3-task-1@appspot.gserviceaccount.com
app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)
app.secret_key = "01234567"
bucket_name = "task-1-images"
datastore_client = datastore.Client()
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)
AEST = timezone(datetime.timedelta(hours=10))
log_client = gcloud_logging.Client()
log_client.get_default_handler()
log_client.setup_logging()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    handlers=[logging.StreamHandler()],
)


def get_current_time_aest():
    return DateTime.now(AEST)


@app.route("/")
def root():
    if "username" in session:
        logging.info(f"User {session['username']} logged in. Redirecting to forum.")
        return redirect(url_for("forum"))
    else:
        logging.info("User not logged in. Redirecting to login page.")
        return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        Username = request.form.get("Username", "").strip()
        Password = request.form.get("Password", "").strip()
        if not Username or not Password:
            if not Username:
                logging.warning("Invalid login attempt with empty username.")
                return render_template("index.html", error="Username is required.")
            if not Password:
                logging.warning("Invalid login attempt with empty password.")
                return render_template("index.html", error="Password is required.")
        try:
            query = datastore_client.query(kind="user")
            query.add_filter("user_name", "=", Username)
            user_data = list(query.fetch(limit=1))
            user = next(iter(user_data), None)
            if user and user["password"] == Password:
                session["username"] = Username
                session["id"] = user["id"]
                logging.info(f"User {Username} logged in successfully.")
                return redirect(url_for("forum"))
            else:
                logging.warning(f"Invalid login attempt for username: {Username}")
                return render_template(
                    "index.html", error="Username or password is invalid."
                )
        except Exception as e:
            logging.error(f"Error during login: {e}")
            return render_template(
                "index.html", error="Internal error. Please try again later."
            )
    else:
        return render_template("index.html")


@app.route("/register")
def register():
    logging.info("Accessed registration page.")
    return render_template("register.html")


@app.route("/register_submit", methods=["GET", "POST"])
def register_submit():
    if request.method == "POST":
        try:
            ID = request.form.get("ID", "").strip()
            Username = request.form.get("Username", "").strip()
            Password = request.form.get("Password", "").strip()
            if not ID or not Username or not Password:
                if not ID:
                    logging.warning("Invalid registration attempt with empty ID.")
                    return render_template("register.html", error="ID is required.")
                if not Username:
                    logging.warning("Invalid registration attempt with empty username.")
                    return render_template(
                        "register.html", error="Username is required."
                    )
                if not Password:
                    logging.warning("Invalid registration attempt with empty password.")
                    return render_template(
                        "register.html", error="Password is required."
                    )

            # Validate image.
            uploaded_file = request.files.get("image")
            if uploaded_file and uploaded_file.filename:
                # Check the file extension.
                if not uploaded_file.filename.lower().endswith((".jpg", ".jpeg")):
                    return render_template(
                        "register.html", error="Invalid image format."
                    )

                # Check the file size.
                if uploaded_file.content_length > 1 * 1024 * 1024:
                    return render_template(
                        "register.html", error="Image must be less than 1MB."
                    )

                # Check the image size.
                image = Image.open(uploaded_file)
                if image.size > (120, 120):
                    return render_template(
                        "register.html", error="Image must be less than 120x120 pixels."
                    )

            # Check if ID exists in Datastore.
            id_query = datastore_client.query(kind="user")
            id_query.add_filter("id", "=", ID)
            if list(id_query.fetch(1)):
                return render_template("register.html", error="The ID already exists.")

            # Check if Username exists in Datastore.
            username_query = datastore_client.query(kind="user").add_filter(
                "user_name", "=", Username
            )
            if list(username_query.fetch(1)):
                return render_template(
                    "register.html", error="The username already exists."
                )

            # Store ID, Username, and Password in Datastore.
            incomplete_key = datastore_client.key("user")
            new_entity = datastore.Entity(key=incomplete_key)
            new_entity.update(
                {
                    "id": ID,
                    "user_name": Username,
                    "password": Password,
                    "image_url": None,
                    "file_extension": None,
                }
            )
            datastore_client.put(new_entity)

            # Upload image to Google Storage, if provided.
            # Check if it's a jpg/jpeg file.
            if uploaded_file and allowed_file(uploaded_file.filename):
                # Grab the file extension: ".jpg", ".jpeg" to store it.
                file_extension = os.path.splitext(uploaded_file.filename)[1].lower()
                blob = bucket.blob(f"users/{ID}{file_extension}")
                uploaded_file.seek(0)
                blob.upload_from_string(
                    uploaded_file.read(), content_type=uploaded_file.content_type
                )

            # Update entity with image URL and extension, if provided.
            if uploaded_file:
                expiration_time = datetime.timedelta(hours=1)
                new_entity["image_url"] = f"users/{ID}{file_extension}"
                logging.info(f"Uploaded image for user with ID: {ID}")
            datastore_client.put(new_entity)
            logging.info(f"Registered new user with ID: {ID}, Username: {Username}")
            return render_template("index.html", message="Registered.")
        except Exception as e:
            logging.error(f"Error processing registration: {e}")
            return render_template(
                "index.html",
                error=f"Not registered.",
            )


@app.route("/message_submit", methods=["GET", "POST"])
def message_submit():
    if request.method == "POST":
        try:
            Subject = request.form.get("Subject", "").strip()
            Message = request.form.get("Message", "").strip()
            uploaded_file = request.files.get("image")

            # Input validation.
            if not Subject or not Message:
                return render_template(
                    "forum.html", error="Subject and Message are required."
                )

            # Setting some length limits.
            if len(Subject) > 200 or len(Message) > 1000:
                return render_template(
                    "forum.html", error="Subject or Message is too long."
                )

            ID = escape(session["id"])
            dateTime = get_current_time_aest().isoformat()
            file_extension = None

            # Store Subject and Message in Datastore.
            incomplete_key = datastore_client.key("message")
            new_entity = datastore.Entity(key=incomplete_key)
            new_entity.update(
                {
                    "id": ID,
                    "datetime": dateTime,
                    "subject": Subject,
                    "message": Message,
                    "user_name": session.get("username", "Anonymous"),
                    "image_url": None,
                    "file_extension": None,
                }
            )
            datastore_client.put(new_entity)

            # Get the unique ID assigned by Datastore.
            post_unique_id = str(new_entity.key.id)

            # Upload image to Google Storage, if provided.
            # Check if it's a jpg/jpeg file.
            if uploaded_file and allowed_file(uploaded_file.filename):
                # Grab the file extension: ".jpg", ".jpeg" to store it.
                # This is so we ren't hardcoding the file extension.
                file_extension = os.path.splitext(uploaded_file.filename)[1].lower()
                uploaded_file.seek(0)
                blob = bucket.blob(f"{ID}/{post_unique_id}{file_extension}")
                blob.upload_from_string(
                    uploaded_file.read(), content_type=uploaded_file.content_type
                )

                # Update entity with image URL and extension.
                expiration_time = datetime.timedelta(hours=1)
                new_entity[
                    "image_url"
                ] = f"{session['id']}/{post_unique_id}{file_extension}"
                new_entity["file_extension"] = file_extension
                datastore_client.put(new_entity)
            logging.info(f"Stored message for user ID: {ID}")
            return redirect(url_for("forum"))
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return render_template(
                "forum.html",
                error=f"Unable to process message submission.",
            )


def allowed_file(filename):
    ALLOWED_EXTENSIONS = {"jpg", "jpeg"}
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route("/forum")
def forum():
    if "username" not in session:
        message = "You are not logged in."
        return render_template("index.html", message=message)
    username = escape(session["username"])
    ID = escape(session["id"])

    # Get the signed URL for the logged-in user's image
    user_image_url = get_signed_user_image_url(ID, datetime.timedelta(hours=1))
    try:
        # Fetch the last 10 posts.
        query = datastore_client.query(kind="message")
        query.order = ["-datetime"]
        posts = list(query.fetch(10))
        for post in posts:
            # Fetch user data associated with post's id.
            user_query = datastore_client.query(kind="user")
            user_query.add_filter("id", "=", post["id"])
            user_data = list(user_query.fetch(limit=1))

            # Generate signed URLs.
            # For post images.
            try:
                if post.get("image_url"):
                    post["image_url"] = get_signed_url_from_image_url(
                        post["id"], post["image_url"], datetime.timedelta(hours=1)
                    )
            except KeyError:
                image_url = "path_to_default_placeholder_image"

            # For user images.
            if user_data:
                raw_user_image_url = user_data[0].get("image_url")
                if raw_user_image_url:
                    user_image_filename = raw_user_image_url.split("/")[-1]
                    user_image_path = "users"
                    post["user_image_url"] = get_signed_url_from_image_url(
                        user_image_path,
                        user_image_filename,
                        datetime.timedelta(hours=1),
                    )
                else:
                    None
        logging.info(posts)
        return render_template(
            "forum.html",
            username=username,
            id=ID,
            posts=posts,
            image_url=user_image_url,
        )
    except Exception as e:
        error = str(e)
        logging.error(f"Error fetching posts: {e}")
        return render_template("index.html", error=f"Error fetching posts.")


@app.route("/user")
def user():
    if "username" not in session:
        message = "You are not logged in."
        return render_template("index.html", message=message)

    # Get the signed URL for the logged-in user's image
    ID = escape(session["id"])
    user_image_url = get_signed_user_image_url(ID, datetime.timedelta(hours=1))
    try:
        username = escape(session["username"])
        ID = escape(session["id"])

        # Fetch user's posts.
        query = datastore_client.query(kind="message")
        user_posts_raw = list(query.add_filter("id", "=", ID).fetch())
        user_posts_raw = sorted(
            user_posts_raw, key=lambda x: x["datetime"], reverse=True
        )
        user_posts = []
        for post in user_posts_raw:
            post_data = dict(post)
            post_data["post_unique_id"] = post.key.id
            if "image_url" in post:
                # Construct the signed URL for the image.
                signed_url = get_signed_url_from_image_url(
                    post["id"], post["image_url"], datetime.timedelta(hours=1)
                )
                post_data["image_url"] = signed_url
            user_posts.append(post_data)
        return render_template(
            "user.html",
            username=username,
            id=ID,
            user_posts=user_posts,
            image_url=user_image_url,
        )
    except Exception as e:
        return render_template(
            "index.html",
            error=f"An unexpected error occurred. Please try again later.",
        )


@app.route("/change_password", methods=["POST"])
def change_password():
    if "username" not in session or "id" not in session:
        logging.warning("User is not logged in.")
        return render_template("index.html", error="You are not logged in.")
    old_password = request.form.get("old_password")
    new_password = request.form.get("new_password")
    username = session.get("username")
    id = session.get("id")
    image_url = get_signed_user_image_url(id, datetime.timedelta(hours=1))

    # Validate password inputs.
    if not old_password or not new_password:
        flash("Both old and new passwords are required.")
        return redirect(url_for("user"))
    try:
        query = datastore_client.query(kind="user")
        user = list(query.add_filter("id", "=", session["id"]).fetch(1))
        if not user:
            flash("User not found.")
            return redirect(url_for("user"))

        # Check if the old password matches.
        if user[0]["password"] != old_password:
            flash("The old password is incorrect.")
            return redirect(url_for("user"))

        # Update password.
        user[0]["password"] = new_password
        datastore_client.put(user[0])

        # Log the user out.
        session.pop("username", None)
        session.pop("id", None)
        flash("Password changed. Please log in again.")
        return redirect(url_for("login"))
    except Exception as e:
        logging.info(f"An error occurred while updating the password: {e}")
        flash("An error occurred while updating the password. Please try again.")
        return redirect(url_for("user"))


@app.route("/edit_post/<post_id>")
def edit_post(post_id):
    logging.info(f"Attempting to edit post with ID: {post_id}")
    if "username" not in session or "id" not in session:
        return render_template("index.html", error="You are not logged in.")
    try:
        # Fetch the post by its ID from Datastore.
        key = datastore_client.key("message", int(post_id))
        post = datastore_client.get(key)

        # Ensure the post exists and belongs to the logged in user.
        if not post or post["id"] != session["id"]:
            return render_template(
                "user.html", error="You don't have permission to edit this post."
            )
        post["post_unique_id"] = key.id
        return render_template("edit_post.html", post=post)
    except Exception as e:
        logging.error(f"Error fetching post {post_id}: {e}")
        return render_template(
            "user.html",
            error="An error occurred while fetching the post. Please try again.",
        )


@app.route("/update_post/<post_id>", methods=["POST"])
def update_post(post_id):
    if not is_logged_in():
        return render_template("index.html", error="You are not logged in.")

    post = fetch_post_by_id(post_id)

    if not post:
        logging.warning(f"No post found for post_id: {post_id}")
        return render_template("user.html", error="No post found.")

    if not is_post_owner(post):
        logging.warning(
            f"User {session['username']} attempted to edit a post they don't own."
        )
        return render_template(
            "user.html", error="You don't have permission to edit this post."
        )
    subject, message, uploaded_file = extract_form_data()
    if not subject or not message:
        logging.warning(f"Subject or message missing for post_id: {post_id}")
        return render_template(
            "edit_post.html", post=post, error="Subject and message cannot be empty."
        )
    image_url, error = validate_and_upload_image(uploaded_file, post_id)
    if error:
        logging.error(
            f"Error during image validation for post_id: {post_id}. Error: {error}"
        )
        return render_template("edit_post.html", post=post, error=error)
    if image_url:
        post["image_url"] = image_url
    update_post_data(post, subject, message)
    try:
        datastore_client.put(post)
    except Exception as e:
        logging.error(f"Error saving post to Datastore: {e}")
        return render_template(
            "edit_post.html",
            post=post,
            error="Failed to save the post. Please try again.",
        )
    logging.info(f"Successfully updated the post with post_id: {post_id}")
    return redirect(url_for("forum"))


def is_logged_in():
    return "username" in session and "id" in session


def fetch_post_by_id(post_id):
    key = datastore_client.key("message", int(post_id))
    return datastore_client.get(key)


def is_post_owner(post):
    return post["id"] == session["id"]


def extract_form_data():
    subject = request.form["subject"]
    message = request.form["message"]
    uploaded_file = request.files.get("image")
    return subject, message, uploaded_file


def validate_and_upload_image(uploaded_file, post_id):
    # If no image is uploaded, return early with None values
    if not uploaded_file or not uploaded_file.filename:
        return None, None

    # Check the file extension.
    if not uploaded_file.filename.lower().endswith((".jpg", ".jpeg")):
        return None, "Invalid image format."

    # Check the file size.
    if uploaded_file.content_length > 1 * 1024 * 1024:
        return None, "Image must be less than 1MB."

    # Check the image dimensions.
    image = Image.open(uploaded_file)
    if image.size > (120, 120):
        return None, "Image must be less than 120x120 pixels."

    # If validations pass, upload to GCS.
    if allowed_file(uploaded_file.filename):
        file_extension = os.path.splitext(uploaded_file.filename)[1].lower()
        blob = bucket.blob(f"{session['id']}/{post_id}{file_extension}")
        try:
            uploaded_file.seek(0)
            blob.upload_from_string(
                uploaded_file.read(), content_type=uploaded_file.content_type
            )
        except Exception as e:
            logging.error(f"Error uploading image to GCS: {e}")
            return None, "Failed to upload the image. Please try again."

        # Reset the file pointer to the start after saving
        uploaded_file.seek(0)
        logging.info(f"Successfully uploaded image for post ID: {post_id} to GCS.")
        blob_path = f"{session['id']}/{post_id}{file_extension}"
        try:
            signed_url = generate_signed_url(
                bucket_name, blob_path, datetime.timedelta(hours=1)
            )
            return signed_url, None
        except Exception as e:
            logging.warning(f"Failed to generate signed URL: {e}")
            return "static/placeholder.png", None
    return None, "File type not allowed."


# I was going to make the bucket public but then didn't, so instead of changing the datastore,
# I'll use this utility to make it all better.
def get_signed_url_from_image_url(
    user_id, image_url, expiration_duration, is_user_image=False
):
    if not image_url:
        return None

    # Extract image name. Up to ?, as I stored a signed URL at some point.
    image_name = image_url.split("/")[-1].split("?")[0]
    if is_user_image:
        image_path = f"users/{image_name}"
    else:
        image_path = f"{user_id}/{image_name}"
    expiration_time = datetime.timedelta(hours=1)
    try:
        return generate_signed_url(bucket_name, image_path, expiration_time)
    except Exception as e:
        logging.warning(f"Failed to generate signed URL: {e}")
        url = "static/placeholder.png"
        return url


def generate_signed_url(bucket_name, image_name, expiration_time):
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(image_name)
    try:
        url = blob.generate_signed_url(
            expiration=expiration_time, method="GET", version="v4"
        )
        return url
    except Exception as e:
        logging.warning(f"Failed to generate signed URL: {e}")
        url = "static/placeholder.png"
        return url


# Gets the signed URL for the logged in user's image to pass to a page.
def get_signed_user_image_url(user_id, expiration_duration):
    user_query = datastore_client.query(kind="user")
    user_query.add_filter("id", "=", user_id)
    user_data = list(user_query.fetch(limit=1))
    if user_data:
        raw_user_image_url = user_data[0].get("image_url")
        if raw_user_image_url:
            image_name = raw_user_image_url.split("/")[-1]
            signed_url = get_signed_url_from_image_url(
                "users", image_name, expiration_duration, is_user_image=True
            )
            return signed_url

    # Return None if there's no image for the user.
    return None


def update_post_data(post, subject, message):
    post["subject"] = subject
    post["message"] = message
    post["datetime"] = get_current_time_aest().isoformat()


@app.route("/logout", methods=["POST"])
def logout():
    try:
        session.pop("username", None)
        session.pop("id", None)
        flash("Logged out.")
        return redirect(url_for("login"))
    except Exception as e:
        logging.error(f"Error during logout: {e}")
        return render_template(
            "index.html", error="An error occurred during logout. Please try again."
        )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)

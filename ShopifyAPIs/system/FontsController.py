from database.DataController import *
from utils.UtilsController import *
from utils.LogsController import *

class FontsController(DataController):
    def __init__(self):
        super().__init__()
        self.utils = UtilsController()
        self.log = LogsController()
        self.repository_name = self.utils.get_REPOSITORY_NAME()

    class Font:
        def __init__(self):
            self.id = None
            self.title = None
            self.name = None
            self.extension = None
            self.font_size = None
            self.file_url = None

        # Setters
        def set_id(self, id):
            self.id = id

        def set_title(self, title):
            self.title = title

        def set_name(self, name):
            self.name = name

        def set_extension(self, extension):
            self.extension = extension

        def set_font_size(self, font_size):
            self.font_size = font_size

        def set_file_url(self, file_url):
            self.file_url = file_url

        # Getters
        def get_id(self):
            return self.id

        def get_title(self):
            return self.title

        def get_name(self):
            return self.name

        def get_extension(self):
            return self.extension

        def get_font_size(self):
            return self.font_size

        def get_file_url(self):
            return self.file_url

    # API Functions
    def upsert_font(self, font_id, font_json, function="insert"):
        # print(f"\n[INFO] BEGIN - Adding Font")
        result_flag = False
        return_code = 200
        result_file_name = None
        result_url = None
        result_error = None
        result_string = None
        new_file_name = None
        font = self.Font()
        font_title = None
        font_name = None
        font_size = None
        file_name = None
        file_path = None
        file = None
        font_extension = None
        renamed_file_name = None

        try:
            font_title = str(font_json.get('title')).replace("null", "None")
            font_name = str(font_json.get('name')).replace("null", "None")
            font_size = font_json.get('font_size') if font_json.get('font_size') is not None else 12
            file_name = font_json.get('file_name', None)
            file_path = font_json.get('file_path')

            # Getting file
            if file_name is not None:
                try:
                    file = self.utils.read_file(file_path, file_name)
                except Exception as e:
                    print(f"[ERROR] {e}")
                    return False, 500, str(e)

                if not file:
                    print(f"[ERROR] File '{file_name}' not found")
                    return False, 404, f"File '{file_name}' not found"

                font_extension = f".{file_name.split('.')[-1]}"
            else:
                font_extension = f".{file_path.split('.')[-1]}"

            if font_title is None or font_title == "":
                print(f"[ERROR] Font Title is required")
                return False, 400, "Font Title is required"
            if font_name is None or font_name == "":
                print(f"[ERROR] Font Name is required")
                return False, 400, "Font Name is required"
            if function == "insert" and font_extension is None or font_extension == "":
                print(f"[ERROR] Font Extension is required")
                return False, 400, "Font Extension is required"
            if function == "update" and font_id is None:
                print(f"[ERROR] Font ID is required")
                return False, 400, "Font ID is required"

            if file_name is not None and file_path is not None:
                if font_name != file_name.split('.')[0]:
                    new_file_name = f"{font_name}{font_extension}"
                    result_flag, renamed_file_name = self.utils.rename_file(file_path, file_name, new_file_name)

                    if not result_flag:
                        print(f"[ERROR] Failed to rename file '{file_name}' to '{new_file_name}'")
                        return False, 500, f"Failed to rename file '{file_name}' to '{new_file_name}'"

                    file_name = new_file_name

            font.set_id(font_id)
            font.set_title(font_title)
            font.set_name(font_name)
            font.set_extension(font_extension)
            font.set_font_size(font_size)

            result_flag, return_code, result_query = self.verify_font_exists(font, function)

            if function == "insert" and result_flag:
                print(f"[ERROR] Font '{font_name}' already exists")
                return False, 409, "Font already exists"

            if function == "update" and not result_flag:
                print(f"[ERROR] Font '{font_name}' does not exist")
                return False, 404, "Font does not exist"

            if file_name is not None:
                result_flag, result_file_name, result_url, result_error = self.upload_font_to_s3(file_path=file_path, file_name=file_name)

                font.set_file_url(result_url)

                if not result_flag:
                    print(f"[ERROR] Failed to upload file '{file.filename}' to S3. Error: {result_error}")
                    return False, 500, result_error
            else:
                font.set_file_url(f"{file_path}")

            if function == "update":
                result_flag, return_code, result_string = self.update_font(font)
            else:
                result_flag, result_string = self.insert_font(font)

            if not result_flag:
                print(f"[ERROR] Failed to insert Font '{font_name}'")
                return False, 500, result_string
            else:
                print(f"[INFO] Font '{font_name}' added successfully")
                if file_name is not None and file_path is not None:
                    result_flag = self.utils.delete_files(file_path, None, [file_name], None)

                return True, 200, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished adding Font")
            # print("[INFO] Cleaning up variables")
            try:
                del font, font_id, font_title, font_name, font_size, file_name, file_path, file, font_extension, renamed_file_name, new_file_name, result_flag, return_code, result_file_name, result_url, result_error, result_string
            except:
                pass

    # Database Functions
    def verify_font_exists(self, font:Font, function):
        # print(f"\n[INFO] BEGIN - Verifying Font Exists")
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        total = 0
        result_flag = False
        result_query = None
        title = None
        name = None
        row_id = None

        try:
            row_id = font.get_id()
            title = str(font.get_title()).replace("null", "None")
            name = str(font.get_name()).replace("null", "None")

            if function == "update":
                condition += f"\nAND ROW_ID = '{row_id}'"
            else:
                condition += f"\nAND TITLE = '{title}'"
                condition += f"\nAND NAME = '{name}'"

            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_FONTS(), columns, condition)

            if result_flag:
                for row in result_query:
                    total = row.get('TOTAL')
                    if total > 0:
                        # print(f"[INFO] Font exists")
                        return True, 200, "Font exists"
                    else:
                        # print(f"[INFO] Font does not exist")
                        return False, 404, "Font does not exist"
            else:
                return False, 500, result_query
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished verifying Font Exists")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, total, result_flag, result_query, title, name, row_id
            except:
                pass

    def get_all_fonts(self):
        # print(f"\n[INFO] BEGIN - Getting all fonts...")
        fonts = []
        font = {}
        columns = ["ROW_ID", "TITLE", "NAME", "EXTENSION", "FONT_SIZE", "FILE_URL"]
        condition = "1=1"
        return_code = 200

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_FONTS(), columns, condition)

            if result_flag:
                for row in result_query:
                    font = {
                        "id": row.get('ROW_ID'),
                        "title": row.get('TITLE'),
                        "name": row.get('NAME'),
                        "extension": row.get('EXTENSION'),
                        "font_size": row.get('FONT_SIZE'),
                        "file_path": row.get('FILE_URL')
                    }
                    fonts.append(font)
                return True, return_code, fonts
            else:
                return False, 500, result_query
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished getting all fonts")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, fonts, font
            except:
                pass

    def insert_font(self, font:Font):
        # print(f"\n[INFO] BEGIN - Inserting Font")
        result_flag = False
        result_string = "Success"
        rowcount = 0
        title = None
        name = None
        extension = None
        font_size = None
        file_url = None

        try:
            title = str(font.get_title()).replace("null", "None")
            name = str(font.get_name()).replace("null", "None")
            extension = str(font.get_extension())
            font_size = font.get_font_size() if font.get_font_size() is not None else 12
            file_url = font.get_file_url()

            if title is None or title == "":
                print(f"[ERROR] Font Title is required")
                return False, 400, "Font Title is required"
            if name is None or name == "":
                print(f"[ERROR] Font Name is required")
                return False, 400, "Font Name is required"
            if extension is None or extension == "":
                print(f"[ERROR] Font Extension is required")
                return False, 400, "Font Extension is required"

            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("TITLE", self.utils.replace_special_chars(title))
            self.utils.validate_columns_values("NAME", self.utils.replace_special_chars(name))
            self.utils.validate_columns_values("EXTENSION", extension)
            self.utils.validate_columns_values("FONT_SIZE", font_size)
            self.utils.validate_columns_values("FILE_URL", file_url)

            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_CUSTOM_FONTS(), self.utils.get_columns_array(), self.utils.get_values_array())

            if result_flag:
                print(f"[INFO] Font {title} inserted successfully")
                return True, result_string
            else:
                print(f"[ERROR] Error inserting Font {title}")
                return False, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished inserting Font")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, rowcount, title, name, extension, font_size, file_url
            except:
                pass

    def update_font(self, font:Font):
        # print(f"\n[INFO] BEGIN - Updating Font")
        result_flag = False
        result_string = "Success"
        condition = "1=1"
        rowcount = 0

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("TITLE", self.utils.replace_special_chars(str(font.get_title()).replace("null", "None")))
            self.utils.validate_columns_values("NAME", self.utils.replace_special_chars(str(font.get_name()).replace("null", "None")))
            self.utils.validate_columns_values("EXTENSION", font.get_extension())
            self.utils.validate_columns_values("FONT_SIZE", font.get_font_size())
            self.utils.validate_columns_values("FILE_URL", font.get_file_url())

            if font.get_id() is None:
                print(f"[ERROR] Font ID is required")
                return False, 400, "Font ID is required"
            else:
                condition += f"\nAND ROW_ID = '{font.get_id()}'"

            result_flag, rowcount, result_string = super().update_record(super().get_tbl_CUSTOM_FONTS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if result_flag:
                print(f"[INFO] Font {font.get_name()} updated successfully")
                return True, 200, result_string
            else:
                print(f"[ERROR] Error updating Font {font.get_font_name()}")
                return False, 500, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished updating Font")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, rowcount
            except:
                pass

    def delete_font(self, font_id):
        # print(f"\n[INFO] BEGIN - Deleting Font")
        result_flag = False
        result_string = "Success"
        condition = "1=1"
        condition += f"\nAND ROW_ID = '{font_id}'"
        rowcount = 0

        if font_id is None:
            print(f"[ERROR] Font ID is required")
            return False, 400, "Font ID is required"

        try:
            result_flag, rowcount, result_string = super().delete_record(super().get_tbl_CUSTOM_FONTS(), condition)

            if result_flag:
                print(f"[INFO] Font {font_id} deleted successfully")
                return True, 200, rowcount, result_string
            else:
                print(f"[ERROR] Error deleting Font {font_id}")
                return False, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, rowcount, str(e)
        finally:
            # print(f"[INFO] END - Finished deleting Font")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, rowcount, condition
            except:
                pass

    def upload_font_to_s3(self, file_path, file_name):
        # print(f"\n[INFO] BEGIN - Uploading Font to S3")
        results = {}
        result_flag = False
        result_error = None
        result_url = None
        result_file_name = None

        try:
            results = self.utils.upload_files_to_s3(file_directory=file_path, file_names=[file_name], bucket_name='COMPANY_NAME-customs-files')

            for result in results:
                result_flag = result.get('success')
                result_error = result.get('error')
                result_url = result.get('url')
                result_file_name = result.get('file_name')

            if result_flag:
                # print(f"[INFO] Font '{result_file_name}' uploaded successfully")
                return result_flag, result_file_name, result_url, result_error
            else:
                # print(f"[ERROR] Failed to upload Font '{result_file_name}'")
                return result_flag, result_file_name, result_url, result_error

        except Exception as e:
            print(f"[ERROR] {e}")
            return False, None, None, str(e)
        finally:
            # print(f"[INFO] END - Finished uploading Font to S3")
            # print("[INFO] Cleaning up variables")
            try:
                del results, result_flag, result_error, result_url, result_file_name
            except:
                pass



import os

from superset.security import SupersetSecurityManager


SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET__SQLALCHEMY_DATABASE_URI"]


class SafeSecurityManager(SupersetSecurityManager):
    def load_user(self, user_id):
        try:
            user_pk = int(user_id)
        except (TypeError, ValueError):
            return None

        user = self.get_user_by_id(user_pk)
        if user and user.is_active:
            return user

        return None


CUSTOM_SECURITY_MANAGER = SafeSecurityManager
from sqlalchemy.ext.asyncio import (AsyncSession, async_sessionmaker,
                                    create_async_engine)


class Database:
    def __init__(self, user, password, host, port, db_name):
        self.connect_parameters = f"{user}:{password}@{host}:{port}/{db_name}"
        self.SYNC_CONNECTION_STRING = f"postgresql+psycopg2://{self.connect_parameters}"
        self.ASYNC_CONNECTION_STRING = f"postgresql+asyncpg://{self.connect_parameters}"
        self.engine = create_async_engine(self.ASYNC_CONNECTION_STRING)
        self.async_session = async_sessionmaker(self.engine)
        self.session: AsyncSession = None

    async def connect(self):
        if self.session is None:
            self.session = self.async_session(expire_on_commit=False)

    async def disconnect(self):
        if self.session is not None:
            await self.session.close()
            self.session = None

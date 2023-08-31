# The Google File System (GFS)

This is a simple implementation written in golang.

## About the Google File System

The Google File System (GFS) is a distributed file system that features
scalability, consistency, reliability, and high performance. It was
designed to store and access data concurrently. It provides usual operations
to create, delete, open, close, read, and write files. And it also supports
some special operations such as atomic record append and snapshot.

With the fact that the metadata has nothing to do with the data, GFS
separates the control flow from the data flow. The control flow, stored in
the master, is all about the metadata. The data flow, stored in chunkservers,
is all about the data.

For more details about GFS, view the
[explaining GFS document](docs/explain_gfs.md).

## Development Documents

All development documents are in the [docs/dev](docs/dev) directory, view the
[overview document for development](docs/dev/overview.md) for details.

## LICENSE

GFS-Go - A simple implementation of the Google File System (GFS)

Copyright (C) 2023 Lau Yee-Yu

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

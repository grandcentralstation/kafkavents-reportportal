# Copyright 2021 Jonathan Holloway <loadtheaccumulator@gmail.com>
#
# This module is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this software. If not, see <http://www.gnu.org/licenses/>.
#
"""ReportPortal module for all things specific to ReportPortal."""
import posixpath
import requests

from reportportal_client import ReportPortalService


class ReportPortal:
    """Handle all things ReportPortal."""

    def __init__(self, config):
        """Initialize the ReportPortal object."""
        self.endpoint = config.get('RP_HOST', None)
        self.project = config.get('RP_PROJECT', None)
        self.api_token = config.get('RP_TOKEN', None)
        self._service = None

    @property
    def service(self):
        """Get the service and create if does not exist."""
        # creating service on first call to get service
        if self._service is None:
            self._service = ReportPortalService(endpoint=self.endpoint,
                                                project=self.project,
                                                token=self.api_token)
            self._service.session.verify = False

        return self._service

    # TODO: get rid of this when .service works
    def connect(self):
        """Connect to a ReportPortal instance."""
        self.service = ReportPortalService(endpoint=self.endpoint,
                                           project=self.project,
                                           token=self.api_token,
                                           is_skipped_an_issue=False)
        self.service.session.verify = False

        print(f'Conversing with ReportPortal {self.endpoint} '
              f'on project {self.project}')

        return self.service

    def api_get(self, api_path, get_data=None, verify=False):
        """GET from the ReportPortal API

        Args:
            get_data (list): list of key=value pairs

        Returns:
            session response object
        """
        url = posixpath.join(self.endpoint, 'api/v1/',
                             self.project, api_path)
        if get_data is not None:
            get_string = '?{}'.format("&".join(get_data))
            url += get_string
        print('url: %s', url)

        session = requests.Session()
        session.headers["Authorization"] = "bearer {0}".format(self.api_token)
        session.headers["Accept"] = "application/json"

        response = session.get(url, verify=verify)

        print('r.status_code: %s', response.status_code)
        print('r.text: %s', response.text)

        return response

// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// inner health check
pub struct InnerHealthCheck {
    pub current_height: u64,
    // only above retry_limit allow broadcast retry, retry timing is 1, 1, 2, 4, 8...2^n
    pub retry_limit: u64,
    // tick count interval times
    pub tick: u64,
}

impl Default for InnerHealthCheck {
    fn default() -> Self {
        Self {
            current_height: u64::MAX,
            retry_limit: 0,
            tick: 0,
        }
    }
}

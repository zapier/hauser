//Package database ...
package database

import (
	"github.com/fullstorydev/hauser/config"
)

//Redshift ...
type Redshift struct {
}

//New returns a new redshift database
func New(c *config.Config) Database {
	return nil
}

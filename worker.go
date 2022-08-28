package worker

import (
	"log"
	"os/exec"
	"time"

	"github.com/Nguyen-Hoa/wattsup"
	"github.com/gin-gonic/gin"
)

/* TODO
Include k8s api to be able to start jobs? No this is handled by manager!
Will we need k8s api at all?
*/

var g_meter = wattsup.Wattsup{}

func dockerExecute(cid string) {
	command := []string{"run", "-d", "--name", cid, "--rm", cid}
	exec.Command(command[0], command[1:]...)
}

func main() {
	r := gin.Default()

	r.GET("/stats", func(c *gin.Context) {
		/* psutil, top, or whatever stats required by models
		perhaps the requests (manager) can specify the parameters
		required to support a wider range of models.
		*/
	})

	r.POST("/meter-start", func(c *gin.Context) {

		// Check if another meter is running
		if g_meter.Running() {
			c.JSON(500, gin.H{
				"message": "Another meter is already running!",
			})
			c.Abort()
		} else {

			// Initialize power meter
			port := "ttyUSB0"
			command := []string{"./wattsup", port, "-g", "watts"}
			// filename := "out.watts"
			t := time.Now()
			filename := t.Format("2006-01-02_15:04:05") + ".watts"
			if err := g_meter.Init(port, filename, command); err != nil {
				log.Fatal("Failed to start new power meter")
				c.JSON(500, gin.H{
					"message": "Failed to start new power meter",
				})
				c.Abort()
			}

			// Start power meter
			g_meter.Start()
			c.JSON(200, gin.H{
				"message": "Power meter started",
			})
		}
	})

	r.POST("/meter-stop", func(c *gin.Context) {
		if err := g_meter.Stop(); err != nil {
			c.JSON(500, gin.H{
				"message": "Failed to stop power meter",
			})
			c.Abort()
		}
		g_meter = wattsup.Wattsup{}
		c.JSON(200, gin.H{
			"message": "Power meter stopped",
		})
	})

	r.POST("/execute", func(c *gin.Context) {
		// get container
		// verify image exists
		// start container
	})

	r.POST("/migrate", func(c *gin.Context) {
		// stop
		// save
		// copy
		// done
	})

	r.POST("/kill", func(c *gin.Context) {
		// get container
		// verfiy contaienr is running
		// kill
	})

	r.Run()
}

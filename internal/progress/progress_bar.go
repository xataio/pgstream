// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"fmt"
	"os"

	"github.com/schollz/progressbar/v3"
	"golang.org/x/term"
)

type Bar interface {
	Add(int) error
	Add64(int64) error
	Current() int64
	SetCurrent(int64) error
	Close() error
}

type ProgressBar struct {
	*progressbar.ProgressBar
}

func (pb *ProgressBar) Current() int64 {
	return pb.ProgressBar.State().CurrentNum
}

func (pb *ProgressBar) SetCurrent(value int64) error {
	return pb.Set64(value)
}

// noopBar is returned when stderr is not a terminal. The progress bar's
// carriage returns and ANSI codes are noise when output is piped to a file or
// log shipper, so we drop the bar entirely in that case.
type noopBar struct{}

func (noopBar) Add(int) error          { return nil }
func (noopBar) Add64(int64) error      { return nil }
func (noopBar) Current() int64         { return 0 }
func (noopBar) SetCurrent(int64) error { return nil }
func (noopBar) Close() error           { return nil }

// stderrIsTTY reports whether the bar's output sink (stderr) is a terminal.
// Indirected through a variable so tests can override it.
var stderrIsTTY = func() bool {
	return term.IsTerminal(int(os.Stderr.Fd()))
}

func NewBar(total int64, description, unit string) Bar {
	if !stderrIsTTY() {
		return noopBar{}
	}
	return &ProgressBar{
		ProgressBar: progressbar.NewOptions64(total,
			progressbar.OptionShowCount(),
			progressbar.OptionShowIts(),
			progressbar.OptionSetItsString(unit),
			progressbar.OptionSetRenderBlankState(true),
			progressbar.OptionSetWidth(20),
			progressbar.OptionSetPredictTime(true),
			progressbar.OptionEnableColorCodes(true),
			progressbar.OptionShowElapsedTimeOnFinish(),
			progressbar.OptionSetDescription(description),
			progressbar.OptionOnCompletion(func() {
				fmt.Printf("\n") //nolint:forbidigo
			}),
			progressbar.OptionSetTheme(progressbar.Theme{
				Saucer:        "[green]=[reset]",
				SaucerHead:    "[green]>[reset]",
				SaucerPadding: " ",
				BarStart:      "[",
				BarEnd:        "]",
			}),
		),
	}
}

func NewBytesBar(totalBytes int64, description string) Bar {
	if !stderrIsTTY() {
		return noopBar{}
	}
	return &ProgressBar{
		ProgressBar: progressbar.NewOptions64(totalBytes,
			progressbar.OptionShowCount(),
			progressbar.OptionSetRenderBlankState(true),
			progressbar.OptionSetPredictTime(true),
			progressbar.OptionSetWidth(20),
			progressbar.OptionEnableColorCodes(true),
			progressbar.OptionShowBytes(true),
			progressbar.OptionShowTotalBytes(true),
			progressbar.OptionShowElapsedTimeOnFinish(),
			progressbar.OptionSetDescription(description),
			progressbar.OptionOnCompletion(func() {
				fmt.Printf("\n") //nolint:forbidigo
			}),
			progressbar.OptionSetTheme(progressbar.Theme{
				Saucer:        "[green]=[reset]",
				SaucerHead:    "[green]>[reset]",
				SaucerPadding: " ",
				BarStart:      "[",
				BarEnd:        "]",
			}),
		),
	}
}

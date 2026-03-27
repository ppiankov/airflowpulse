package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/config"
)

// Run starts the TUI application.
func Run(cfg *config.Config) error {
	var clients []*airflow.Client
	for _, u := range cfg.APIURLs {
		clients = append(clients, airflow.New(u, cfg.APIUser, cfg.APIPassword))
	}

	m := model{
		cfg:     cfg,
		clients: clients,
		health:  make(map[string]*airflow.HealthResponse),
		dagRuns: make(map[string][]airflow.DAGRun),
		pools:   make(map[string][]airflow.Pool),
		errors:  make(map[string][]airflow.ImportError),
	}

	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

type model struct {
	cfg       *config.Config
	clients   []*airflow.Client
	active    int
	health    map[string]*airflow.HealthResponse
	dagRuns   map[string][]airflow.DAGRun
	pools     map[string][]airflow.Pool
	errors    map[string][]airflow.ImportError
	filter    string
	filtering bool
	width     int
	height    int
	scroll    int
	updated   time.Time
	err       error
}

type tickMsg time.Time

type dataMsg struct {
	instance string
	health   *airflow.HealthResponse
	dagRuns  []airflow.DAGRun
	pools    []airflow.Pool
	errors   []airflow.ImportError
	err      error
}

func (m model) Init() tea.Cmd {
	return tea.Batch(m.fetchAll(), tick(m.cfg.PollInterval))
}

func tick(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m model) fetchAll() tea.Cmd {
	cmds := make([]tea.Cmd, len(m.clients))
	for i, c := range m.clients {
		cmds[i] = fetchInstance(c)
	}
	return tea.Batch(cmds...)
}

func fetchInstance(client *airflow.Client) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		instance := config.InstanceLabel(client.BaseURL())
		msg := dataMsg{instance: instance}

		h, err := client.Health(ctx)
		if err != nil {
			msg.err = err
			return msg
		}
		msg.health = h

		runs, err := client.ListDAGRuns(ctx, "~", 100)
		if err == nil {
			msg.dagRuns = runs.DAGRuns
		}

		pools, err := client.ListPools(ctx)
		if err == nil {
			msg.pools = pools.Pools
		}

		ie, err := client.ListImportErrors(ctx)
		if err == nil {
			msg.errors = ie.ImportErrors
		}

		return msg
	}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if m.filtering {
			switch msg.String() {
			case "enter", "esc":
				m.filtering = false
			case "backspace":
				if len(m.filter) > 0 {
					m.filter = m.filter[:len(m.filter)-1]
				}
			default:
				if len(msg.String()) == 1 {
					m.filter += msg.String()
				}
			}
			return m, nil
		}
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "tab":
			if len(m.clients) > 1 {
				m.active = (m.active + 1) % len(m.clients)
			}
		case "/":
			m.filtering = true
			m.filter = ""
		case "up", "k":
			if m.scroll > 0 {
				m.scroll--
			}
		case "down", "j":
			m.scroll++
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

	case tickMsg:
		return m, tea.Batch(m.fetchAll(), tick(m.cfg.PollInterval))

	case dataMsg:
		if msg.err != nil {
			m.err = msg.err
		} else {
			m.health[msg.instance] = msg.health
			m.dagRuns[msg.instance] = msg.dagRuns
			m.pools[msg.instance] = msg.pools
			m.errors[msg.instance] = msg.errors
			m.updated = time.Now()
			m.err = nil
		}
	}

	return m, nil
}

// Styles.
var (
	titleStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("39"))
	headerStyle  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("252"))
	passStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	warnStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	failStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	dimStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	sectionStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("105"))
)

func (m model) View() string {
	if m.width == 0 {
		return "Loading..."
	}

	var b strings.Builder

	// Header.
	instance := ""
	if len(m.clients) > 0 {
		instance = config.InstanceLabel(m.clients[m.active].BaseURL())
	}
	b.WriteString(titleStyle.Render("airflowpulse"))
	b.WriteString("  ")
	b.WriteString(headerStyle.Render(instance))
	if len(m.clients) > 1 {
		b.WriteString(dimStyle.Render(fmt.Sprintf("  [%d/%d Tab:switch]", m.active+1, len(m.clients))))
	}
	b.WriteString("\n")

	if !m.updated.IsZero() {
		b.WriteString(dimStyle.Render(fmt.Sprintf("Updated: %s  Poll: %s", m.updated.Format("15:04:05"), m.cfg.PollInterval)))
	}
	if m.err != nil {
		b.WriteString("  ")
		b.WriteString(failStyle.Render(fmt.Sprintf("Error: %v", m.err)))
	}
	b.WriteString("\n\n")

	// Scheduler.
	b.WriteString(sectionStyle.Render("Scheduler"))
	b.WriteString("\n")
	if h, ok := m.health[instance]; ok {
		schedIcon := passStyle.Render("+")
		if h.Scheduler.Status != "healthy" {
			schedIcon = failStyle.Render("x")
		}
		b.WriteString(fmt.Sprintf("  %s Scheduler: %s", schedIcon, h.Scheduler.Status))
		if h.Scheduler.LatestHeartbeat != "" {
			if t, err := time.Parse(time.RFC3339, h.Scheduler.LatestHeartbeat); err == nil {
				age := time.Since(t).Truncate(time.Second)
				ageStr := dimStyle.Render(fmt.Sprintf("  heartbeat %s ago", age))
				if age > 30*time.Second {
					ageStr = warnStyle.Render(fmt.Sprintf("  heartbeat %s ago", age))
				}
				b.WriteString(ageStr)
			}
		}
		b.WriteString("\n")

		dbIcon := passStyle.Render("+")
		if h.Metadatabase.Status != "healthy" {
			dbIcon = failStyle.Render("x")
		}
		b.WriteString(fmt.Sprintf("  %s Metadatabase: %s\n", dbIcon, h.Metadatabase.Status))
	} else {
		b.WriteString(dimStyle.Render("  waiting for data...\n"))
	}
	b.WriteString("\n")

	// Pools.
	b.WriteString(sectionStyle.Render("Pools"))
	b.WriteString("\n")
	if pools, ok := m.pools[instance]; ok && len(pools) > 0 {
		b.WriteString(fmt.Sprintf("  %-20s %6s %6s %6s %6s\n",
			headerStyle.Render("NAME"),
			headerStyle.Render("USED"),
			headerStyle.Render("QUEUE"),
			headerStyle.Render("OPEN"),
			headerStyle.Render("TOTAL")))
		for _, p := range pools {
			openStr := fmt.Sprintf("%6d", p.OpenSlots)
			suffix := ""
			if p.OpenSlots == 0 && p.QueuedSlots > 0 {
				openStr = failStyle.Render(openStr)
				suffix = failStyle.Render(" !!")
			}
			b.WriteString(fmt.Sprintf("  %-20s %6d %6d %s %6d%s\n",
				p.Name, p.RunningSlots, p.QueuedSlots, openStr, p.Slots, suffix))
		}
	} else {
		b.WriteString(dimStyle.Render("  no pool data\n"))
	}
	b.WriteString("\n")

	// DAG Runs.
	b.WriteString(sectionStyle.Render("DAG Runs"))
	if m.filtering {
		b.WriteString(fmt.Sprintf("  filter: %s_", m.filter))
	} else if m.filter != "" {
		b.WriteString(dimStyle.Render(fmt.Sprintf("  (filter: %s)", m.filter)))
	}
	b.WriteString("\n")
	if runs, ok := m.dagRuns[instance]; ok && len(runs) > 0 {
		// Group by dag_id.
		type dagSummary struct {
			states map[string]int
			order  int
		}
		dagMap := make(map[string]*dagSummary)
		var dagOrder []string
		for i, r := range runs {
			if m.filter != "" && !strings.Contains(r.DagID, m.filter) {
				continue
			}
			if _, exists := dagMap[r.DagID]; !exists {
				dagMap[r.DagID] = &dagSummary{states: make(map[string]int), order: i}
				dagOrder = append(dagOrder, r.DagID)
			}
			dagMap[r.DagID].states[r.State]++
		}

		if len(dagOrder) > 0 {
			b.WriteString(fmt.Sprintf("  %-30s %8s %8s %8s %8s\n",
				headerStyle.Render("DAG"),
				headerStyle.Render("running"),
				headerStyle.Render("queued"),
				headerStyle.Render("success"),
				headerStyle.Render("failed")))
			displayed := 0
			for _, dagID := range dagOrder {
				if displayed >= 20 {
					b.WriteString(dimStyle.Render(fmt.Sprintf("  ... and %d more DAGs\n", len(dagOrder)-displayed)))
					break
				}
				s := dagMap[dagID]
				name := dagID
				if len(name) > 30 {
					name = name[:27] + "..."
				}
				failedStr := fmt.Sprintf("%8d", s.states["failed"])
				if s.states["failed"] > 0 {
					failedStr = failStyle.Render(failedStr)
				}
				b.WriteString(fmt.Sprintf("  %-30s %8d %8d %8d %s\n",
					name, s.states["running"], s.states["queued"], s.states["success"], failedStr))
				displayed++
			}
		} else {
			b.WriteString(dimStyle.Render("  no matching DAG runs\n"))
		}
	} else {
		b.WriteString(dimStyle.Render("  no DAG run data\n"))
	}
	b.WriteString("\n")

	// Import errors.
	b.WriteString(sectionStyle.Render("Import Errors"))
	b.WriteString("\n")
	if errs, ok := m.errors[instance]; ok && len(errs) > 0 {
		b.WriteString(failStyle.Render(fmt.Sprintf("  %d import error(s)\n", len(errs))))
		for i, e := range errs {
			if i >= 5 {
				b.WriteString(dimStyle.Render(fmt.Sprintf("  ... and %d more\n", len(errs)-5)))
				break
			}
			b.WriteString(fmt.Sprintf("  %s %s\n", failStyle.Render("x"), e.Filename))
		}
	} else {
		b.WriteString(passStyle.Render("  no import errors\n"))
	}
	b.WriteString("\n")

	// Footer.
	b.WriteString(dimStyle.Render("q:quit  /:filter  Tab:instance  j/k:scroll"))
	b.WriteString("\n")

	return b.String()
}

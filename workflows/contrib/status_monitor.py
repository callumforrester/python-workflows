from __future__ import absolute_import, division
import curses
import time
import workflows.transport
import threading

class Monitor():

  shutdown = False
  '''Set to true to end the main loop and shut down the service monitor.'''

  cards = {}
  '''Register card shown for seen services'''

  def __init__(self, transport=None):
    '''Set up monitor and connect to the network transport layer'''
    if transport is None or isinstance(transport, basestring):
      self._transport = workflows.transport.lookup(transport)()
    else:
      self._transport = transport()
    assert self._transport.connect(), "Could not connect to transport layer"
    self._lock = threading.RLock()
    self._node_status = {}
    self.message_box = None
    self._transport.subscribe_broadcast('transient.status.demo', self.update_status, retroactive=True)

  def update_status(self, header, message):
    '''Process incoming status message. Acquire lock for status dictionary before updating.'''
    with self._lock:
      if self.message_box:
        self.message_box.erase()
        self.message_box.move(0,0)
        for n, field in enumerate(header):
          if n == 0:
            self.message_box.addstr(field + ":", curses.color_pair(1))
          else:
            self.message_box.addstr(", " + field + ":", curses.color_pair(1))
          self.message_box.addstr(header[field])
        self.message_box.addstr(": ", curses.color_pair(1))
        self.message_box.addstr(str(message), curses.color_pair(2) + curses.A_BOLD)
        self.message_box.refresh()
      if message['host'] not in self._node_status or \
          int(header['timestamp']) >= self._node_status[message['host']]['last_seen']:
        self._node_status[message['host']] = message
        self._node_status[message['host']]['last_seen'] = int(header['timestamp'])

  def run(self):
    '''A wrapper for the real _run() function to cleanly enable/disable the
       curses environment.'''
    curses.wrapper(self._run)

  @staticmethod
  def _boxwin(height, width, row, column, title=None, title_x=7, color_pair=None):
    box = curses.newwin(height, width, row, column)
    box.clear()
    if color_pair:
      box.attron(curses.color_pair(color_pair))
    box.box()
    if title:
      box.addstr(0, title_x, " " + title + " ")
    if color_pair:
      box.attroff(curses.color_pair(color_pair))
    box.noutrefresh()
    return curses.newwin(height - 2, width - 2, row + 1, column + 1)

  def _redraw_screen(self, stdscr):
    '''Redraw screen. This could be to initialize, or to redraw after resizing.'''
    with self._lock:
      stdscr.clear()
      stdscr.addstr(0, 0, "workflows service monitor -- quit with Ctrl+C", curses.A_BOLD)
      stdscr.refresh()
      self.message_box = self._boxwin(5, curses.COLS, 2, 0, title='last seen message', color_pair=1)
      self.message_box.scrollok(True)
      self.cards = []

  def _get_card(self, number):
    with self._lock:
      if number < len(self.cards):
        return self.cards[number]
      if number == len(self.cards):
        self.cards.append(self._boxwin(6, 35, 7, 35 * number, color_pair=3))
        return self.cards[number]
      raise RuntimeError("Card number too high")

  def _erase_card(self, number):
    '''Destroy cards with this or higher number.'''
    with self._lock:
      if number < (len(self.cards) - 1):
        self._erase_card(number + 1)
      if number > (len(self.cards) - 1):
        return
      obliterate = curses.newwin(6, 35, 7, 35 * number)
      obliterate.erase()
      obliterate.noutrefresh()
      del(self.cards[number])

  def _run(self, stdscr):
    '''Start the actual service monitor'''
    curses.use_default_colors()
    curses.curs_set(False)
    curses.init_pair(1, curses.COLOR_RED, -1)
    curses.init_pair(2, curses.COLOR_BLACK, -1)
    curses.init_pair(3, curses.COLOR_GREEN, -1)
    self._redraw_screen(stdscr)

    try:
      while not self.shutdown:
        now = int(time.time())
        with self._lock:
          overview = self._node_status.copy()
        cardnumber = 0
        for host, status in overview.iteritems():
          age = (now - int(status['last_seen'] / 1000))
          if age > 90:
            with self._lock:
              del(self._node_status[host])
          else:
            card = self._get_card(cardnumber)
            card.erase()
            card.move(0, 0)
            card.addstr('Host: ', curses.color_pair(3))
            card.addstr(host)
            card.move(1, 0)
            card.addstr('Service: ', curses.color_pair(3))
            card.addstr(status['service'])
            card.move(2, 0)
            card.addstr('State: ', curses.color_pair(3))
            card.addstr(str(status['status']))
            card.move(3, 0)
            if age < 10:
              card.addstr("status is current")
            else:
              card.addstr("last seen %d seconds ago" % age, curses.color_pair(1))
            card.noutrefresh()
            cardnumber = cardnumber + 1
        if cardnumber < len(self.cards):
          self._erase_card(cardnumber)
        curses.doupdate()
        time.sleep(0.2)
    except KeyboardInterrupt:
      '''User pressed CTRL+C'''
      pass
    self._transport.disconnect()

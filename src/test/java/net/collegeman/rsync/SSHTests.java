package test.java.net.collegeman.rsync;

import java.awt.Container;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

import junit.framework.TestCase;
import ch.ethz.ssh2.Connection;

public class SSHTests extends TestCase {
	
	private static String password = null;
	
	private static String username = null;
	
	private static String host = "localhost";
	
	private static Connection conn;

	public void setUp() throws Exception {
		if (username == null) {
			promptForSettings();
		}
	}
	
	public void testConnect() throws IOException {
		conn = new Connection(host);
		conn.connect();
		assertTrue(conn.authenticateWithPassword(username, password));
	}
	
	public static void promptForSettings() throws InterruptedException {
		
		JFrame window = new JFrame();
		window.setSize(new Dimension(300, 200));
		window.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		Container pane = window.getContentPane();
		GridBagLayout layout = new GridBagLayout();
		pane.setLayout(layout);
		
		GridBagConstraints c = new GridBagConstraints();
		c.anchor = GridBagConstraints.LINE_START;
		
		final JLabel usernameLabel = new JLabel("Username");
		c.gridx = 0;
		c.gridy = 0;
		pane.add(usernameLabel, c);
		
		final JTextField usernameField = new JTextField("acollegeman", 15);
		c.gridx = 0;
		c.gridy = 1;
		pane.add(usernameField, c);
		
		final JLabel passwordLabel = new JLabel("Password");
		c.gridx = 1;
		c.gridy = 0;
		pane.add(passwordLabel, c);
		
		final JPasswordField passwordField = new JPasswordField("", 15);
		c.gridx = 1;
		c.gridy = 1;
		pane.add(passwordField, c);
		
		
		final JButton connect = new JButton("Connect");
		c.gridx = 1;
		c.gridy = 2;
		c.anchor = GridBagConstraints.LINE_END;
		pane.add(connect, c);
		
		window.pack();
		
		connect.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				username = usernameField.getText();
				password = new String(passwordField.getPassword());
			}
		});
		
		window.setVisible(true);
		
		while (username == null) {
			Thread.sleep(1000);
		}
	
	}
	
}

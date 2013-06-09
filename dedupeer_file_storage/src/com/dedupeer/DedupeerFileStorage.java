package com.dedupeer;

import java.awt.Dimension;
import java.awt.Rectangle;
import java.awt.Robot;
import java.awt.Toolkit;
import java.awt.image.BufferedImage;
import java.io.File;

import javax.imageio.ImageIO;
import javax.swing.SwingUtilities;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

import com.dedupeer.gui.MainGUI;
import com.dedupeer.thrift.ThriftServer;


/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class DedupeerFileStorage {
		
	public static void main (String[] args) {
		PropertyConfigurator.configure("resources/log4j.properties");
		LogManager.getRootLogger().setLevel((Level)Level.INFO);
		
		ThriftServer srv = new ThriftServer();
		Thread tSrv = new Thread(srv);
		tSrv.start();
		
		SwingUtilities.invokeLater(new Runnable(){
			@Override
			public void run() {
				new MainGUI();				
			}
		});
		

		Thread screenRecord = new Thread(new Runnable() {
			@Override
			public void run() {
				int count = 0;
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				while(true) {
					try {
						Toolkit tk = Toolkit.getDefaultToolkit();
				        Dimension tamanho = tk.getScreenSize();
				        Rectangle screenRect = new Rectangle(tamanho);
				        Robot r = new Robot();						
				        BufferedImage screenCapturedImage = r.createScreenCapture(screenRect);
				        ImageIO.write(screenCapturedImage, "jpg", new File("e:\\screenshots\\32_deduplicacao_rehidratacao_" + count + ".jpg"));						
				        count++;
				        Thread.sleep(60000);
					} catch (Exception e) {				
						e.printStackTrace();
					}
				}
			}
		});
		//screenRecord.start();
	}
}
package com.dedupeer.gui.component;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;

import javax.swing.JComponent;

import com.dedupeer.utils.Range;

public class ModificationBar extends JComponent {

	private static final long serialVersionUID = 3032628316015934754L;
	
	private Color colorNormalArea = Color.gray;
	private Color colorModifiedArea = Color.blue;
	
	private ArrayList<Range> ranges;
	private long fileLength;
	
	private int componenteWidth = 300;
	private int componenteHeight = 40;
	
	public ModificationBar(ArrayList<Range> ranges, long fileLength) {
		super();
		this.setPreferredSize(new Dimension(componenteWidth, componenteHeight));		
		this.ranges = ranges;
		this.fileLength = fileLength;
	}
	
	@Override
	public void paintComponent(Graphics g) {
		 Graphics2D g2 = (Graphics2D) g;
		 g2.setColor(colorNormalArea);
		 g2.fillRect(0, 0, this.getWidth(), componenteHeight);
		 
		 g2.setColor(colorModifiedArea);
		 for(Range range: ranges) {
			 int intialPosition = (int) ((((range.getInitialValue()*100)/fileLength)/100f) * componenteWidth);
			 int finalPosition = (int) ((((range.getFinalValue()*100)/fileLength)/100f) * componenteWidth);
			 g2.fillRect(intialPosition, 0, finalPosition - intialPosition, componenteHeight);
		 }
	}

}
